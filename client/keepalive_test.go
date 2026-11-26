//go:build !windows

package memcached

import (
	"crypto/tls"
	"net"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// Linux ABI values for the per-socket keepalive options, from include/uapi/linux/tcp.h. They
// are hard-coded because darwin has no TCP_KEEPIDLE at all and gives TCP_KEEPINTVL/TCP_KEEPCNT
// different values, so syscall.TCP_KEEPIDLE would not compile on every unix this test builds
// for. Only ever used behind the runtime.GOOS == "linux" guard in assertKeepAlive. SOL_SOCKET
// and SO_KEEPALIVE are taken from the syscall package instead, since those do vary by
// architecture and are defined on every unix.
const (
	linuxTCPKeepIdle  = 4
	linuxTCPKeepIntvl = 5
	linuxTCPKeepCnt   = 6
)

// dialLoopback returns a client wrapped around a real TCP connection to a listener that accepts
// and holds. If useTLS is set, the connection is wrapped in a *tls.Conn without handshaking -
// enough to exercise the transport unwrap, which is all keepalive cares about.
func dialLoopback(t *testing.T, useTLS bool) (*Client, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()

	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		ln.Close()
		t.Fatalf("dial: %v", err)
	}

	var transport MemcachedConnection = raw.(*net.TCPConn)
	if useTLS {
		transport = tls.Client(raw, &tls.Config{InsecureSkipVerify: true})
	}

	client, err := Wrap(transport)
	if err != nil {
		raw.Close()
		ln.Close()
		t.Fatalf("Wrap: %v", err)
	}

	return client, func() {
		raw.Close()
		ln.Close()
		if peer, ok := <-accepted; ok {
			peer.Close()
		}
	}
}

// TestKeepAliveTCPConnUnwrapsTLS is the regression guard for the defect that made both keepalive
// tiers a no-op on any TLS-enabled cluster: SetKeepAliveOptions used to assert
// c.conn.(*net.TCPConn) directly, which fails for the *tls.Conn that ConnectTLS produces. The
// failure was silent - no error, no log, just no keepalive - so nothing but this test catches a
// regression.
func TestKeepAliveTCPConnUnwrapsTLS(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		name := "plaintext"
		if useTLS {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			client, cleanup := dialLoopback(t, useTLS)
			defer cleanup()

			if client.keepAliveTCPConn() == nil {
				t.Fatal("keepAliveTCPConn returned nil; keepalive would be a silent no-op")
			}
		})
	}
}

// A non-TCP transport must be tolerated rather than panicking.
func TestKeepAliveNonTCPTransport(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	client, err := Wrap(a)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}

	if conn := client.keepAliveTCPConn(); conn != nil {
		t.Errorf("keepAliveTCPConn on a net.Pipe returned %v, expected nil", conn)
	}

	client.SetKeepAliveOptions(5 * time.Second)
	if err := client.SetKeepAliveConfig(net.KeepAliveConfig{Enable: true, Idle: 5 * time.Second}); err != nil {
		t.Errorf("SetKeepAliveConfig on a non-TCP transport = %v, expected nil", err)
	}
}

// TestSetKeepAliveConfigThroughTLS confirms the settings actually reach the kernel socket, by
// reading them back with getsockopt. Linux only: macOS exposes the equivalents under different
// names and units, and there is no portable readback.
func TestSetKeepAliveConfigThroughTLS(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("keepalive socket options are not portably readable on %s", runtime.GOOS)
	}

	cfg := net.KeepAliveConfig{
		Enable:   true,
		Idle:     7 * time.Second,
		Interval: 2 * time.Second,
		Count:    3,
	}

	for _, useTLS := range []bool{false, true} {
		name := "plaintext"
		if useTLS {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			client, cleanup := dialLoopback(t, useTLS)
			defer cleanup()

			if err := client.SetKeepAliveConfig(cfg); err != nil {
				t.Fatalf("SetKeepAliveConfig: %v", err)
			}
			assertKeepAlive(t, client.keepAliveTCPConn(), cfg)
		})
	}
}

// assertKeepAlive reads TCP_KEEPIDLE/TCP_KEEPINTVL/TCP_KEEPCNT back off the socket and checks
// they match cfg. All three matter: Go's SetKeepAlivePeriod sets only TCP_KEEPIDLE, leaving the
// probe schedule at the OS default (75s x 9 on Linux), which adds ~11 minutes to worst case
// detection no matter how low the idle threshold is.
func assertKeepAlive(t *testing.T, conn *net.TCPConn, cfg net.KeepAliveConfig) {
	t.Helper()

	if conn == nil {
		t.Fatal("no underlying *net.TCPConn")
	}

	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}

	opts := []struct {
		name  string
		level int
		num   int
		exp   int
	}{
		{"SO_KEEPALIVE", syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1},
		{"TCP_KEEPIDLE", syscall.IPPROTO_TCP, linuxTCPKeepIdle, int(cfg.Idle.Seconds())},
		{"TCP_KEEPINTVL", syscall.IPPROTO_TCP, linuxTCPKeepIntvl, int(cfg.Interval.Seconds())},
		{"TCP_KEEPCNT", syscall.IPPROTO_TCP, linuxTCPKeepCnt, cfg.Count},
	}

	var ctrlErr error
	err = raw.Control(func(fd uintptr) {
		for _, o := range opts {
			got, gerr := syscall.GetsockoptInt(int(fd), o.level, o.num)
			if gerr != nil {
				ctrlErr = gerr
				return
			}
			if got != o.exp {
				t.Errorf("%s = %d, expected %d", o.name, got, o.exp)
			}
		}
	})
	if err != nil {
		t.Fatalf("Control: %v", err)
	}
	if ctrlErr != nil {
		t.Fatalf("getsockopt: %v", ctrlErr)
	}
}

// TestSetKeepAliveConfigRecorded checks that the last applied configuration is recorded, so that
// connection pools that apply the same configuration on every checkout don't pay three or four
// setsockopt calls each time, and that SetKeepAliveOptions, which changes the socket outside that
// record, invalidates it.
func TestSetKeepAliveConfigRecorded(t *testing.T) {
	active := net.KeepAliveConfig{Enable: true, Idle: 5 * time.Second, Interval: 2 * time.Second, Count: 3}
	pooled := active
	pooled.Idle = 30 * time.Minute

	for _, useTLS := range []bool{false, true} {
		name := "plaintext"
		if useTLS {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			client, cleanup := dialLoopback(t, useTLS)
			defer cleanup()

			for _, cfg := range []net.KeepAliveConfig{active, active, pooled, active} {
				if err := client.SetKeepAliveConfig(cfg); err != nil {
					t.Fatalf("SetKeepAliveConfig(%v): %v", cfg, err)
				}
				if !client.keepAliveSet || client.keepAlive != cfg {
					t.Fatalf("recorded %v (set %v), expected %v", client.keepAlive, client.keepAliveSet, cfg)
				}
			}

			client.SetKeepAliveOptions(time.Minute)
			if client.keepAliveSet {
				t.Fatalf("SetKeepAliveOptions left %v recorded", client.keepAlive)
			}
			if err := client.SetKeepAliveConfig(active); err != nil || !client.keepAliveSet || client.keepAlive != active {
				t.Fatalf("re-apply after SetKeepAliveOptions: err %v, recorded %v", err, client.keepAlive)
			}
			// SetKeepAliveOptions left TCP_KEEPIDLE at 60s: reading back the active values shows the
			// re-apply really reached the socket rather than only updating the record
			if runtime.GOOS == "linux" {
				assertKeepAlive(t, client.keepAliveTCPConn(), active)
			}
		})
	}
}

// A non-TCP transport has nothing to apply, so nothing is recorded.
func TestSetKeepAliveConfigNonTCPNotRecorded(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	client, err := Wrap(a)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if err := client.SetKeepAliveConfig(net.KeepAliveConfig{Enable: true, Idle: 5 * time.Second}); err != nil {
		t.Fatalf("SetKeepAliveConfig: %v", err)
	}
	if client.keepAliveSet {
		t.Errorf("non-TCP transport recorded %v", client.keepAlive)
	}
}

// The zero configuration is a real request (Enable false: disable keepalive), not "nothing
// recorded", so it must reach the socket even on a fresh Client. net.Dial enables keepalive by
// default, so a skipped call would leave SO_KEEPALIVE on.
func TestSetKeepAliveConfigZeroDisables(t *testing.T) {
	client, cleanup := dialLoopback(t, false)
	defer cleanup()

	if err := client.SetKeepAliveConfig(net.KeepAliveConfig{}); err != nil {
		t.Fatalf("SetKeepAliveConfig: %v", err)
	}
	if !client.keepAliveSet {
		t.Fatal("zero configuration not recorded")
	}

	raw, err := client.keepAliveTCPConn().SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}
	var got int
	var gerr error
	if err := raw.Control(func(fd uintptr) {
		got, gerr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_KEEPALIVE)
	}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if gerr != nil {
		t.Fatalf("getsockopt: %v", gerr)
	}
	if got != 0 {
		t.Errorf("SO_KEEPALIVE = %d after applying the zero configuration, expected 0", got)
	}
}
