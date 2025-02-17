// Package memcached provides a memcached binary protocol client.
package memcached

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gomemcached"
	"github.com/couchbase/goutils/logging"
	"github.com/couchbase/goutils/scramsha"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type ClientIface interface {
	Add(vb uint16, key string, flags int, exp int, body []byte, context ...*ClientContext) (*gomemcached.MCResponse, error)
	Append(vb uint16, key string, data []byte, context ...*ClientContext) (*gomemcached.MCResponse, error)
	Auth(user, pass string) (*gomemcached.MCResponse, error)
	AuthList() (*gomemcached.MCResponse, error)
	AuthPlain(user, pass string) (*gomemcached.MCResponse, error)
	AuthScramSha(user, pass string) (*gomemcached.MCResponse, error)
	CASNext(vb uint16, k string, exp int, state *CASState) bool
	CAS(vb uint16, k string, f CasFunc, initexp int) (*gomemcached.MCResponse, error)
	CollectionsGetCID(scope string, collection string) (*gomemcached.MCResponse, error)
	CollectionEnabled() bool
	Close() error
	Decr(vb uint16, key string, amt, def uint64, exp int, context ...*ClientContext) (uint64, error)
	Del(vb uint16, key string, context ...*ClientContext) (*gomemcached.MCResponse, error)
	EnableMutationToken() (*gomemcached.MCResponse, error)
	EnableFeatures(features Features) (*gomemcached.MCResponse, error)
	EnableDataPool(getter func(uint64) ([]byte, error), doneCb func([]byte)) error
	Get(vb uint16, key string, context ...*ClientContext) (*gomemcached.MCResponse, error)
	GetAllVbSeqnos(vbSeqnoMap map[uint16]uint64, context ...*ClientContext) (map[uint16]uint64, error)
	GetAndTouch(vb uint16, key string, exp int, context ...*ClientContext) (*gomemcached.MCResponse, error)
	GetBulk(vb uint16, keys []string, rv map[string]*gomemcached.MCResponse, subPaths []string, context ...*ClientContext) error
	GetCollectionsManifest() (*gomemcached.MCResponse, error)
	GetMeta(vb uint16, key string, context ...*ClientContext) (*gomemcached.MCResponse, error)
	GetRandomDoc(context ...*ClientContext) (*gomemcached.MCResponse, error)
	GetSubdoc(vb uint16, key string, subPaths []string, context ...*ClientContext) (*gomemcached.MCResponse, error)
	SetSubdoc(vb uint16, key string, ops []SubDocOp, addOnly bool, exp int, cas uint64, context ...*ClientContext) (
		*gomemcached.MCResponse, error)
	Hijack() MemcachedConnection
	Incr(vb uint16, key string, amt, def uint64, exp int, context ...*ClientContext) (uint64, error)
	LastBucket() string
	Observe(vb uint16, key string) (result ObserveResult, err error)
	ObserveSeq(vb uint16, vbuuid uint64) (result *ObserveSeqResult, err error)
	Receive() (*gomemcached.MCResponse, error)
	ReceiveWithDeadline(deadline time.Time) (*gomemcached.MCResponse, error)
	Replica() bool
	Send(req *gomemcached.MCRequest) (rv *gomemcached.MCResponse, err error)
	Set(vb uint16, key string, flags int, exp int, body []byte, context ...*ClientContext) (*gomemcached.MCResponse, error)
	SetKeepAliveOptions(interval time.Duration)
	SetReadDeadline(t time.Time)
	SetDeadline(t time.Time)
	SetReplica(r bool)
	SelectBucket(bucket string) (*gomemcached.MCResponse, error)
	SetCas(vb uint16, key string, flags int, exp int, cas uint64, body []byte, context ...*ClientContext) (
		*gomemcached.MCResponse, error)
	Stats(key string) ([]StatValue, error)
	StatsFunc(key string, fn func(key, val []byte)) error
	StatsMap(key string) (map[string]string, error)
	StatsMapForSpecifiedStats(key string, statsMap map[string]string) error
	Transmit(req *gomemcached.MCRequest) error
	TransmitWithDeadline(req *gomemcached.MCRequest, deadline time.Time) error
	TransmitResponse(res *gomemcached.MCResponse) error
	UprGetFailoverLog(vb []uint16) (map[uint16]*FailoverLog, error)
	GetConnName() string
	SetConnName(name string)

	// UprFeed Related
	NewUprFeed() (*UprFeed, error)
	NewUprFeedIface() (UprFeedIface, error)
	NewUprFeedWithConfig(ackByClient bool) (*UprFeed, error)
	NewUprFeedWithConfigIface(ackByClient bool) (UprFeedIface, error)

	CreateRangeScan(vb uint16, start []byte, excludeStart bool, end []byte, excludeEnd bool, withDocs bool,
		context ...*ClientContext) (*gomemcached.MCResponse, error)
	CreateRandomScan(vb uint16, sampleSize int, withDocs bool, context ...*ClientContext) (*gomemcached.MCResponse, error)
	ContinueRangeScan(vb uint16, uuid []byte, opaque uint32, items uint32, maxSize uint32, timeout uint32,
		context ...*ClientContext) error
	CancelRangeScan(vb uint16, uuid []byte, opaque uint32, context ...*ClientContext) (*gomemcached.MCResponse, error)

	ValidateKey(vb uint16, key string, context ...*ClientContext) (bool, error)

	GetErrorMap(errMapVersion gomemcached.ErrorMapVersion) (map[string]interface{}, error)
}

type ClientContext struct {
	// Collection-based context
	CollId uint32

	// Impersonate context
	User string

	// VB-state related context
	// nil means not used in this context
	VbState *VbStateType

	// Preserve Expiry
	PreserveExpiry bool

	// Durability Level
	DurabilityLevel gomemcached.DurabilityLvl

	// Durability Timeout
	DurabilityTimeout time.Duration

	// Data is JSON in snappy compressed format
	Compressed bool

	// Sub-doc paths are document fields (not XATTRs)
	DocumentSubDocPaths bool

	// Include XATTRs in random document retrieval
	IncludeXATTRs bool
}

func (this *ClientContext) Copy() *ClientContext {
	rv := &ClientContext{
		CollId:            this.CollId,
		User:              this.User,
		PreserveExpiry:    this.PreserveExpiry,
		DurabilityLevel:   this.DurabilityLevel,
		DurabilityTimeout: this.DurabilityTimeout,
		Compressed:        this.Compressed,
	}

	rv.VbState = new(VbStateType)

	if this.VbState != nil {
		*rv.VbState = *this.VbState
	}

	return rv
}

type VbStateType uint8

const (
	VbAlive   VbStateType = 0x00
	VbActive  VbStateType = 0x01
	VbReplica VbStateType = 0x02
	VbPending VbStateType = 0x03
	VbDead    VbStateType = 0x04
)

const RandomScanSeed = 0x5eedbead

var (
	ErrUnSuccessfulHello          = errors.New("Unsuccessful HELLO exchange")
	ErrInvalidHello               = errors.New("Invalid HELLO response")
	ErrPreserveExpiryNotSupported = errors.New("PreserveExpiry is not supported")
	ErrDurabilityNotSupported     = errors.New("Durability is not supported")
)

func (context *ClientContext) InitExtras(req *gomemcached.MCRequest, client *Client) {
	if req == nil || client == nil {
		return
	}

	var bytesToAllocate int
	switch req.Opcode {
	case gomemcached.GET_ALL_VB_SEQNOS:
		if context.VbState != nil {
			bytesToAllocate += 4
		}
		if client.CollectionEnabled() {
			if context.VbState == nil {
				bytesToAllocate += 8
			} else {
				bytesToAllocate += 4
			}
		}
	}
	if bytesToAllocate > 0 {
		req.Extras = make([]byte, bytesToAllocate)
	}
}

type SubDocOp struct {
	Xattr   bool
	Path    string
	Value   []byte
	Counter bool
}

func (this *SubDocOp) encodedLength() int {
	return 8 + len([]byte(this.Path)) + len(this.Value)
}

func (this *SubDocOp) encode(buf []byte) []byte {
	if this.Counter {
		buf = append(buf, byte(gomemcached.SUBDOC_COUNTER))
	} else if this.Value == nil {
		buf = append(buf, byte(gomemcached.SUBDOC_DELETE))
	} else if this.Path == "" && !this.Xattr {
		buf = append(buf, byte(gomemcached.SET))
	} else {
		buf = append(buf, byte(gomemcached.SUBDOC_DICT_UPSERT))
	}
	if this.Xattr {
		buf = append(buf, byte(gomemcached.SUBDOC_FLAG_XATTR))
	} else {
		buf = append(buf, byte(0))
	}

	pathBytes := []byte(this.Path)

	buf = binary.BigEndian.AppendUint16(buf, uint16(len(pathBytes)))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(this.Value)))

	buf = append(buf, pathBytes...)
	if this.Value != nil {
		buf = append(buf, this.Value...)
	}

	return buf
}

const bufsize = 1024

var UnHealthy uint32 = 0
var Healthy uint32 = 1

type Features []Feature
type Feature uint16

const (
	FeatureTcpNoDelay        = Feature(0x03)
	FeatureMutationToken     = Feature(0x04) // XATTR bit in data type field with dcp mutations
	FeatureXattr             = Feature(0x06)
	FeatureXerror            = Feature(0x07)
	FeatureSnappyCompression = Feature(0x0a)
	FeatureDataType          = Feature(0x0b) // This is named as JSON in kv_engine's feature.h
	FeatureTracing           = Feature(0x0f)
	FeatureSyncReplication   = Feature(0x11)
	FeatureCollections       = Feature(0x12)
	FeatureSnappyEverywhere  = Feature(0x13)
	FeaturePreserveExpiry    = Feature(0x14)
	FeatureComputeUnits      = Feature(0x1a)
	FeatureHandleThrottle    = Feature(0x1b)
)

type MemcachedConnection interface {
	io.ReadWriteCloser

	SetReadDeadline(time.Time) error
	SetDeadline(time.Time) error
}

// The Client itself.
type Client struct {
	conn MemcachedConnection
	// use uint32 type so that it can be accessed through atomic APIs
	healthy uint32
	opaque  uint32

	hdrBuf []byte

	collectionsEnabled uint32
	enabledFeatures    map[Feature]bool
	replica            bool
	deadline           time.Time
	bucket             string
	// If set, this takes precedence over the global variable ConnName
	connName string

	objPoolEnabled uint32
	datapoolGetter func(uint64) ([]byte, error)
	datapoolDone   func([]byte)
}

var (
	// ConnName is used if Client.connName is not set
	ConnName           = "GoMemcached"
	DefaultDialTimeout = time.Duration(0) // No timeout

	DefaultWriteTimeout = time.Duration(0) // No timeout

	dialFun = func(prot, dest string) (net.Conn, error) {
		return net.DialTimeout(prot, dest, DefaultDialTimeout)
	}

	datapoolDisabled = uint32(0)
	datapoolInit     = uint32(1)
	datapoolInitDone = uint32(2)
)

func SetConnectionName(name string) {
	ConnName = name
}

// Connect to a memcached server.
func Connect(prot, dest string) (rv *Client, err error) {
	conn, err := dialFun(prot, dest)
	if err != nil {
		return nil, err
	}
	return Wrap(conn)
}

// Connect to a memcached server using TLS.
func ConnectTLS(prot, dest string, config *tls.Config) (rv *Client, err error) {
	conn, err := tls.Dial(prot, dest, config)
	if err != nil {
		return nil, err
	}
	return Wrap(conn)
}

func SetDefaultTimeouts(dial, read, write time.Duration) {
	DefaultDialTimeout = dial
	DefaultWriteTimeout = write
}

func SetDefaultDialTimeout(dial time.Duration) {
	DefaultDialTimeout = dial
}

func (c *Client) SetKeepAliveOptions(interval time.Duration) {
	tcpConn, ok := c.conn.(*net.TCPConn)
	if ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(interval)
	}
}

func (c *Client) SetReadDeadline(t time.Time) {
	c.conn.SetReadDeadline(t)
}

func (c *Client) SetDeadline(t time.Time) {
	if t.Equal(c.deadline) {
		return
	}
	c.conn.SetDeadline(t)
	c.deadline = t
}

func (c *Client) getOpaque() uint32 {
	if c.opaque >= math.MaxInt32 {
		c.opaque = uint32(1)
	}
	return c.opaque + 1
}

// Wrap an existing transport.
func Wrap(conn MemcachedConnection) (rv *Client, err error) {
	client := &Client{
		conn:            conn,
		hdrBuf:          make([]byte, gomemcached.HDR_LEN),
		opaque:          uint32(1),
		enabledFeatures: make(map[Feature]bool),
	}
	client.setHealthy(true)
	return client, nil
}

// Close the connection when you're done.
func (c *Client) Close() error {
	return c.conn.Close()
}

// IsHealthy returns true unless the client is belived to have
// difficulty communicating to its server.
//
// This is useful for connection pools where we want to
// non-destructively determine that a connection may be reused.
func (c Client) IsHealthy() bool {
	healthyState := atomic.LoadUint32(&c.healthy)
	return healthyState == Healthy
}

// Send a custom request and get the response.
func (c *Client) Send(req *gomemcached.MCRequest) (rv *gomemcached.MCResponse, err error) {
	err = c.Transmit(req)
	if err != nil {
		return
	}
	resp, _, err := getResponse(c.conn, c.hdrBuf)
	if err == nil && resp.Opaque != req.Opaque {
		logging.Errorf("Send: got response for opaque %v instead of response for opaque %v. req: %v -> res: %v",
			resp.Opaque, req.Opaque, req, resp)
		err = resp
		resp.Status = gomemcached.EINVAL
		c.setHealthy(false)
	} else {
		c.setHealthy(!gomemcached.IsFatal(err))
	}
	return resp, err
}

// Transmit send a request, but does not wait for a response.
func (c *Client) Transmit(req *gomemcached.MCRequest) error {
	if DefaultWriteTimeout > 0 {
		c.conn.(net.Conn).SetWriteDeadline(time.Now().Add(DefaultWriteTimeout))
	}
	_, err := transmitRequest(c.conn, req)
	// clear write deadline to avoid interference with future write operations
	if DefaultWriteTimeout > 0 {
		c.conn.(net.Conn).SetWriteDeadline(time.Time{})
	}
	if err != nil {
		c.setHealthy(false)
	}
	return err
}

func (c *Client) TransmitWithDeadline(req *gomemcached.MCRequest, deadline time.Time) error {
	c.conn.(net.Conn).SetWriteDeadline(deadline)

	_, err := transmitRequest(c.conn, req)

	// clear write deadline to avoid interference with future write operations
	c.conn.(net.Conn).SetWriteDeadline(time.Time{})

	if err != nil {
		c.setHealthy(false)
	}
	return err
}

// TransmitResponse send a response, does not wait.
func (c *Client) TransmitResponse(res *gomemcached.MCResponse) error {
	if DefaultWriteTimeout > 0 {
		c.conn.(net.Conn).SetWriteDeadline(time.Now().Add(DefaultWriteTimeout))
	}
	_, err := transmitResponse(c.conn, res)
	// clear write deadline to avoid interference with future write operations
	if DefaultWriteTimeout > 0 {
		c.conn.(net.Conn).SetWriteDeadline(time.Time{})
	}
	if err != nil {
		c.setHealthy(false)
	}
	return err
}

// Receive a response
func (c *Client) Receive() (*gomemcached.MCResponse, error) {
	var resp *gomemcached.MCResponse
	var err error

	if atomic.LoadUint32(&c.objPoolEnabled) == datapoolInitDone {
		resp, _, err = getResponseWithPool(c.conn, c.hdrBuf, c.datapoolGetter, c.datapoolDone)
	} else {
		resp, _, err = getResponse(c.conn, c.hdrBuf)
	}
	if err != nil && !isNonFatalStatus(resp.Status) {
		c.setHealthy(false)
	}
	return resp, err
}

func (c *Client) ReceiveWithDeadline(deadline time.Time) (*gomemcached.MCResponse, error) {
	c.conn.(net.Conn).SetReadDeadline(deadline)

	resp, _, err := getResponse(c.conn, c.hdrBuf)

	// Clear read deadline to avoid interference with future read operations.
	c.conn.(net.Conn).SetReadDeadline(time.Time{})

	if err != nil && !isNonFatalStatus(resp.Status) {
		c.setHealthy(false)
	}
	return resp, err
}

func isNonFatalStatus(status gomemcached.Status) bool {
	return status == gomemcached.KEY_ENOENT ||
		status == gomemcached.EBUSY ||
		status == gomemcached.RANGE_SCAN_COMPLETE ||
		status == gomemcached.RANGE_SCAN_MORE ||
		status == gomemcached.KEY_EEXISTS ||
		status == gomemcached.WOULD_THROTTLE ||
		status == gomemcached.NOT_MY_VBUCKET ||
		status == gomemcached.SUBDOC_BAD_MULTI
}

func appendMutationToken(bytes []byte) []byte {
	bytes = append(bytes, 0, 0)
	binary.BigEndian.PutUint16(bytes[len(bytes)-2:], uint16(0x04))
	return bytes
}

func (c *Client) GetConnName() string {
	if len(c.connName) > 0 {
		return c.connName
	}
	return ConnName + ":" + uuid.New().String()
}

func (c *Client) SetConnName(name string) {
	c.connName = name
}

// Send a hello command to enable MutationTokens
func (c *Client) EnableMutationToken() (*gomemcached.MCResponse, error) {
	var payload []byte
	payload = appendMutationToken(payload)
	connName := c.GetConnName()

	return c.Send(&gomemcached.MCRequest{
		Opcode: gomemcached.HELLO,
		Key:    []byte(connName),
		Body:   payload,
	})
}

// Send a hello command to enable specific features
func (c *Client) EnableFeatures(features Features) (*gomemcached.MCResponse, error) {
	var payload []byte
	collectionsEnabled := 0
	connName := c.GetConnName()

	for _, feature := range features {
		if feature == FeatureCollections {
			collectionsEnabled = 1
		}
		payload = append(payload, 0, 0)
		binary.BigEndian.PutUint16(payload[len(payload)-2:], uint16(feature))
	}

	rv, err := c.Send(&gomemcached.MCRequest{
		Opcode: gomemcached.HELLO,
		Key:    []byte(connName),
		Body:   payload,
	})

	if err == nil {
		collectionsEnabled = 0
		body := rv.Body
		if rv.Status != gomemcached.SUCCESS {
			logging.Errorf("Client.EnableFeatures: Features can't be enabled: HELLO status = %v", rv.Status)
			return nil, ErrUnSuccessfulHello
		} else if rv.Opcode != gomemcached.HELLO {
			logging.Errorf("Client.EnableFeatures: Invalid memcached HELLO response: opcode %v, expecting %v", rv.Opcode, gomemcached.HELLO)
			return nil, ErrInvalidHello
		} else {
			for i := 0; len(body) > i; i += 2 {
				feature := Feature(binary.BigEndian.Uint16(body[i:]))
				c.enabledFeatures[feature] = true

				if feature == FeatureCollections {
					collectionsEnabled = 1
				}
			}
		}
		atomic.StoreUint32(&c.collectionsEnabled, uint32(collectionsEnabled))
	}
	return rv, err
}

// Sets collection and user info for a request
func (c *Client) setContext(req *gomemcached.MCRequest, context ...*ClientContext) error {
	req.CollIdLen = 0
	req.UserLen = 0
	collectionId := uint32(0)
	collectionsEnabled := atomic.LoadUint32(&c.collectionsEnabled)
	if len(context) > 0 {
		collectionId = context[0].CollId
		uLen := len(context[0].User)

		// we take collections enabled as an indicator that the node understands impersonation
		// since we don't have a specific feature for it.
		if collectionsEnabled > 0 && uLen > 0 && uLen <= gomemcached.MAX_USER_LEN {
			req.UserLen = uLen
			copy(req.Username[:uLen], context[0].User)
		}

		if context[0].PreserveExpiry {
			if !c.IsFeatureEnabled(FeaturePreserveExpiry) {
				return ErrPreserveExpiryNotSupported
			}
			req.FramingExtras = append(req.FramingExtras,
				gomemcached.FrameInfo{gomemcached.FramePreserveExpiry, 0, []byte("")})
		}

		if context[0].DurabilityLevel >= gomemcached.DuraMajority {
			if !c.IsFeatureEnabled(FeatureSyncReplication) {
				return ErrDurabilityNotSupported
			}
			data := make([]byte, 3)
			data[0] = byte(context[0].DurabilityLevel)
			len := 1
			if context[0].DurabilityTimeout > 0 {
				durabilityTimeoutMillis := context[0].DurabilityTimeout / time.Millisecond
				if durabilityTimeoutMillis > math.MaxUint16 {
					durabilityTimeoutMillis = math.MaxUint16
				}
				binary.BigEndian.PutUint16(data[1:3], uint16(durabilityTimeoutMillis))
				len += 2
			}
			req.FramingExtras = append(req.FramingExtras,
				gomemcached.FrameInfo{gomemcached.FrameDurability, len, data})
		}
	}
	// any context with compressed set
	for _, c := range context {
		if c.Compressed {
			req.DataType = gomemcached.DatatypeFlagJSON | gomemcached.DatatypeFlagCompressed
			break
		}
	}

	// if the optional collection is specified, it must be default for clients that haven't turned on collections
	if c.collectionsEnabled == 0 {
		if collectionId != 0 {
			return fmt.Errorf("Client does not use collections but a collection was specified")
		}
	} else {
		req.CollIdLen = binary.PutUvarint(req.CollId[:], uint64(collectionId))
	}
	return nil
}

// Sets collection info in extras
func (c *Client) setExtrasContext(req *gomemcached.MCRequest, context ...*ClientContext) error {
	collectionId := uint32(0)
	xattrs := false
	req.UserLen = 0
	if len(context) > 0 {
		collectionId = context[0].CollId
		uLen := len(context[0].User)
		if uLen > 0 && uLen <= gomemcached.MAX_USER_LEN {
			req.UserLen = uLen
			copy(req.Username[:], context[0].User)
		}
		xattrs = context[0].IncludeXATTRs
	}

	// if the optional collection is specified, it must be default for clients that haven't turned on collections
	if atomic.LoadUint32(&c.collectionsEnabled) == 0 {
		if collectionId != 0 {
			return fmt.Errorf("Client does not use collections but a collection was specified")
		}
	} else {
		if xattrs {
			req.Extras = make([]byte, 5)
			req.Extras[4] = 0x1 // protocol specifies only != 0
		} else {
			req.Extras = make([]byte, 4)
		}
		binary.BigEndian.PutUint32(req.Extras, collectionId)
	}
	return nil
}

func (c *Client) setVbSeqnoContext(req *gomemcached.MCRequest, context ...*ClientContext) error {
	if len(context) == 0 || req == nil {
		return nil
	}

	switch req.Opcode {
	case gomemcached.GET_ALL_VB_SEQNOS:
		if len(context) == 0 {
			return nil
		}

		if len(req.Extras) == 0 {
			context[0].InitExtras(req, c)
		}
		if context[0].VbState != nil {
			binary.BigEndian.PutUint32(req.Extras, uint32(*(context[0].VbState)))
		}
		if c.CollectionEnabled() {
			binary.BigEndian.PutUint32(req.Extras[4:8], context[0].CollId)
		}
		return nil
	default:
		return fmt.Errorf("setVbState Not supported for opcode: %v", req.Opcode.String())
	}
}

// Get the value for a key.
func (c *Client) Get(vb uint16, key string, context ...*ClientContext) (*gomemcached.MCResponse, error) {
	req := &gomemcached.MCRequest{
		VBucket: vb,
		Key:     []byte(key),
		Opaque:  c.getOpaque(),
	}
	if c.replica {
		req.Opcode = gomemcached.GET_REPLICA
	} else {
		req.Opcode = gomemcached.GET
	}
	err := c.setContext(req, context...)
	if err != nil {
		return nil, err
	}
	return c.Send(req)
}

// Get the xattrs, doc value for the input key
func (c *Client) GetSubdoc(vb uint16, key string, subPaths []string, context ...*ClientContext) (*gomemcached.MCResponse, error) {
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.SUBDOC_MULTI_LOOKUP,
		VBucket: vb,
		Key:     []byte(key),
		Opaque:  c.getOpaque(),
	}
	err := c.setContext(req, context...)
	if err != nil {
		return nil, err
	}
	req.Extras, req.Body = GetSubDocVal(subPaths, context)

	res, err := c.Send(req)

	if err != nil && IfResStatusError(res) {
		return res, err
	}
	return res, nil
}

func (c *Client) SetSubdoc(vb uint16, key string, ops []SubDocOp, addOnly bool, exp int, cas uint64, context ...*ClientContext) (
	*gomemcached.MCResponse, error) {

	if len(ops) == 0 {
		return nil, fmt.Errorf("Invalid input - no operations")
	}

	totalBytesLen := 0
	for i := range ops {
		totalBytesLen += ops[i].encodedLength()
	}
	valueBuf := make([]byte, 0, totalBytesLen)
	del := false
	for i := range ops {
		if ops[i].Value == nil {
			del = true
		}
		valueBuf = ops[i].encode(valueBuf)
	}

	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.SUBDOC_MULTI_MUTATION,
		VBucket: vb,
		Key:     []byte(key),
		Extras:  []byte{0, 0, 0, 0, 0},
		Body:    valueBuf,
		Opaque:  c.getOpaque(),
		Cas:     cas,
	}
	err := c.setContext(req, context...)
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(req.Extras, uint32(exp))
	if !del {
		if addOnly {
			req.Extras[4] = gomemcached.SUBDOC_FLAG_ADD
		} else {
			req.Extras[4] = gomemcached.SUBDOC_FLAG_MKDOC
		}
	}

	res, err := c.Send(req)
	if err != nil {
		return nil, err
	} else if !IfResStatusError(res) {
		return nil, fmt.Errorf("operation failed: %v (len=%v)", res.Status, len(res.Body))
	}
	return res, nil
}

// Retrieve the collections manifest.
func (c *Client) GetCollectionsManifest() (*gomemcached.MCResponse, error) {

	res, err := c.Send(&gomemcached.MCRequest{
		Opcode: gomemcached.GET_COLLECTIONS_MANIFEST,
		Opaque: c.getOpaque(),
	})

	if err != nil && IfResStatusError(res) {
		return res, err
	}
	return res, nil
}

// Retrieve the collections manifest.
func (c *Client) CollectionsGetCID(scope string, collection string) (*gomemcached.MCResponse, error) {

	res, err := c.Send(&gomemcached.MCRequest{
		Opcode: gomemcached.COLLECTIONS_GET_CID,
		Body:   []byte(scope + "." + collection),
		Opaque: c.getOpaque(),
	})

	if err != nil && IfResStatusError(res) {
		return res, err
	}
	return res, nil
}

func (c *Client) CollectionEnabled() bool {
	return atomic.LoadUint32(&c.collectionsEnabled) > 0
}

func (c *Client) IsFeatureEnabled(feature Feature) bool {
	enabled, ok := c.enabledFeatures[feature]
	return ok && enabled
}

// Get the value for a key, and update expiry
func (c *Client) GetAndTouch(vb uint16, key string, exp int, context ...*ClientContext) (*gomemcached.MCResponse, error) {
	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], uint32(exp))
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.GAT,
		VBucket: vb,
		Key:     []byte(key),
		Extras:  extraBuf,
		Opaque:  c.getOpaque(),
	}
	err := c.setContext(req, context...)
	if err != nil {
		return nil, err
	}
	return c.Send(req)
}

// Get metadata for a key
func (c *Client) GetMeta(vb uint16, key string, context ...*ClientContext) (*gomemcached.MCResponse, error) {
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.GET_META,
		VBucket: vb,
		Key:     []byte(key),
		Opaque:  c.getOpaque(),
	}
	err := c.setContext(req, context...)
	if err != nil {
		return nil, err
	}
	return c.Send(req)
}

// Del deletes a key.
func (c *Client) Del(vb uint16, key string, context ...*ClientContext) (*gomemcached.MCResponse, error) {
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.DELETE,
		VBucket: vb,
		Key:     []byte(key),
		Opaque:  c.getOpaque(),
	}
	err := c.setContext(req, context...)
	if err != nil {
		return nil, err
	}
	return c.Send(req)
}

// Get a random document
func (c *Client) GetRandomDoc(context ...*ClientContext) (*gomemcached.MCResponse, error) {
	req := &gomemcached.MCRequest{
		Opcode: gomemcached.GET_RANDOM_KEY,
		Opaque: c.getOpaque(),
	}
	err := c.setExtrasContext(req, context...)
	if err != nil {
		return nil, err
	}
	return c.Send(req)
}

// AuthList lists SASL auth mechanisms.
func (c *Client) AuthList() (*gomemcached.MCResponse, error) {
	return c.Send(&gomemcached.MCRequest{
		Opcode: gomemcached.SASL_LIST_MECHS})
}

// Auth performs SASL PLAIN authentication against the server.
func (c *Client) Auth(user, pass string) (*gomemcached.MCResponse, error) {
	res, err := c.AuthList()

	if err != nil {
		return res, err
	}

	authMech := string(res.Body)
	if strings.Index(authMech, "PLAIN") != -1 {
		return c.AuthPlain(user, pass)
	}
	return nil, fmt.Errorf("auth mechanism PLAIN not supported")
}

// AuthScramSha performs SCRAM-SHA authentication against the server.
func (c *Client) AuthScramSha(user, pass string) (*gomemcached.MCResponse, error) {
	res, err := c.AuthList()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to obtain list of methods.")
	}

	methods := string(res.Body)
	method, err := scramsha.BestMethod(methods)
	if err != nil {
		return nil, errors.Wrap(err,
			"Unable to select SCRAM-SHA method.")
	}

	s, err := scramsha.NewScramSha(method)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize scramsha.")
	}

	logging.Infof("Using %v authentication for user %v%v%v", method, gomemcached.UdTagBegin, user, gomemcached.UdTagEnd)

	message, err := s.GetStartRequest(user)
	if err != nil {
		return nil, errors.Wrapf(err,
			"Error building start request for user %s.", user)
	}

	startRequest := &gomemcached.MCRequest{
		Opcode: gomemcached.SASL_AUTH,
		Key:    []byte(method),
		Body:   []byte(message)}

	startResponse, err := c.Send(startRequest)
	if err != nil {
		return nil, errors.Wrap(err, "Error sending start request.")
	}

	err = s.HandleStartResponse(string(startResponse.Body))
	if err != nil {
		return nil, errors.Wrap(err, "Error handling start response.")
	}

	message = s.GetFinalRequest(pass)

	// send step request
	finalRequest := &gomemcached.MCRequest{
		Opcode: gomemcached.SASL_STEP,
		Key:    []byte(method),
		Body:   []byte(message)}
	finalResponse, err := c.Send(finalRequest)
	if err != nil {
		return nil, errors.Wrap(err, "Error sending final request.")
	}

	err = s.HandleFinalResponse(string(finalResponse.Body))
	if err != nil {
		return nil, errors.Wrap(err, "Error handling final response.")
	}

	return finalResponse, nil
}

func (c *Client) AuthPlain(user, pass string) (*gomemcached.MCResponse, error) {
	if len(user) > 0 && user[0] != '@' {
		logging.Infof("Using plain authentication for user %v%v%v", gomemcached.UdTagBegin, user, gomemcached.UdTagEnd)
	}
	return c.Send(&gomemcached.MCRequest{
		Opcode: gomemcached.SASL_AUTH,
		Key:    []byte("PLAIN"),
		Body:   []byte(fmt.Sprintf("\x00%s\x00%s", user, pass))})
}

// select bucket
func (c *Client) SelectBucket(bucket string) (*gomemcached.MCResponse, error) {
	res, err := c.Send(&gomemcached.MCRequest{
		Opcode: gomemcached.SELECT_BUCKET,
		Key:    []byte(bucket)})
	if res != nil {
		c.bucket = bucket
	}
	return res, err
}

func (c *Client) LastBucket() string {
	return c.bucket
}

// Read from replica setting
func (c *Client) SetReplica(r bool) {
	c.replica = r
}

func (c *Client) Replica() bool {
	return c.replica
}

func (c *Client) store(opcode gomemcached.CommandCode, vb uint16,
	key string, flags int, exp int, body []byte, context ...*ClientContext) (*gomemcached.MCResponse, error) {
	req := &gomemcached.MCRequest{
		Opcode:  opcode,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  c.getOpaque(),
		Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
		Body:    body}

	err := c.setContext(req, context...)
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
	return c.Send(req)
}

func (c *Client) storeCas(opcode gomemcached.CommandCode, vb uint16,
	key string, flags int, exp int, cas uint64, body []byte, context ...*ClientContext) (*gomemcached.MCResponse, error) {
	req := &gomemcached.MCRequest{
		Opcode:  opcode,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     cas,
		Opaque:  c.getOpaque(),
		Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
		Body:    body}

	err := c.setContext(req, context...)
	if err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
	return c.Send(req)
}

// Incr increments the value at the given key.
func (c *Client) Incr(vb uint16, key string,
	amt, def uint64, exp int, context ...*ClientContext) (uint64, error) {
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.INCREMENT,
		VBucket: vb,
		Key:     []byte(key),
		Extras:  make([]byte, 8+8+4),
	}
	err := c.setContext(req, context...)
	if err != nil {
		return 0, err
	}

	binary.BigEndian.PutUint64(req.Extras[:8], amt)
	binary.BigEndian.PutUint64(req.Extras[8:16], def)
	binary.BigEndian.PutUint32(req.Extras[16:20], uint32(exp))

	resp, err := c.Send(req)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(resp.Body), nil
}

// Decr decrements the value at the given key.
func (c *Client) Decr(vb uint16, key string,
	amt, def uint64, exp int, context ...*ClientContext) (uint64, error) {
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.DECREMENT,
		VBucket: vb,
		Key:     []byte(key),
		Extras:  make([]byte, 8+8+4),
	}
	err := c.setContext(req, context...)
	if err != nil {
		return 0, err
	}

	binary.BigEndian.PutUint64(req.Extras[:8], amt)
	binary.BigEndian.PutUint64(req.Extras[8:16], def)
	binary.BigEndian.PutUint32(req.Extras[16:20], uint32(exp))

	resp, err := c.Send(req)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(resp.Body), nil
}

// Add a value for a key (store if not exists).
func (c *Client) Add(vb uint16, key string, flags int, exp int,
	body []byte, context ...*ClientContext) (*gomemcached.MCResponse, error) {
	return c.store(gomemcached.ADD, vb, key, flags, exp, body, context...)
}

// Set the value for a key.
func (c *Client) Set(vb uint16, key string, flags int, exp int,
	body []byte, context ...*ClientContext) (*gomemcached.MCResponse, error) {
	return c.store(gomemcached.SET, vb, key, flags, exp, body, context...)
}

// SetCas set the value for a key with cas
func (c *Client) SetCas(vb uint16, key string, flags int, exp int, cas uint64,
	body []byte, context ...*ClientContext) (*gomemcached.MCResponse, error) {
	return c.storeCas(gomemcached.SET, vb, key, flags, exp, cas, body, context...)
}

// Append data to the value of a key.
func (c *Client) Append(vb uint16, key string, data []byte, context ...*ClientContext) (*gomemcached.MCResponse, error) {
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.APPEND,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  c.getOpaque(),
		Body:    data}

	err := c.setContext(req, context...)
	if err != nil {
		return nil, err
	}
	return c.Send(req)
}

// GetBulk gets keys in bulk
func (c *Client) GetBulk(vb uint16, keys []string, rv map[string]*gomemcached.MCResponse, subPaths []string, context ...*ClientContext) error {
	if len(keys) == 1 && len(subPaths) == 0 {
		res, err := c.Get(vb, keys[0], context...)
		if res != nil {
			if res.Status == gomemcached.SUCCESS {
				rv[keys[0]] = res
			} else if res.Status == gomemcached.KEY_ENOENT {

				// GetBulk never returns a ENOENT
				err = nil
			}
		}
		return err
	}

	stopch := make(chan bool)
	var wg sync.WaitGroup

	defer func() {
		close(stopch)
		wg.Wait()
	}()

	if (math.MaxInt32 - c.opaque) < (uint32(len(keys)) + 1) {
		c.opaque = uint32(1)
	}

	opStart := c.opaque

	errch := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logging.Infof("Recovered in f %v", r)
			}
			errch <- nil
			wg.Done()
		}()

		ok := true
		var savedErr error
		for ok {

			select {
			case <-stopch:
				return
			default:
				res, err := c.Receive()

				if err != nil && IfResStatusError(res) {

					// continue receiving in case of KEY_ENOENT and WOULD_THROTTLE
					if res != nil && res.Status == gomemcached.KEY_ENOENT {
						continue
					} else if res != nil && res.Status == gomemcached.WOULD_THROTTLE {

						// if we have bee throttled, flag that there are keys still to fetch
						// the last throttle wins
						savedErr = err
						continue
					} else {
						c.setHealthy(false) // who knows what's left to be received
						errch <- err
						return
					}
				} else if res.Opcode == gomemcached.GET ||
					res.Opcode == gomemcached.GET_REPLICA ||
					res.Opcode == gomemcached.SUBDOC_GET ||
					res.Opcode == gomemcached.SUBDOC_MULTI_LOOKUP {
					opaque := res.Opaque - opStart
					if opaque < 0 || opaque >= uint32(len(keys)) {
						// Every now and then we seem to be seeing an invalid opaque
						// value returned from the server. When this happens log the error
						// and the calling function will retry the bulkGet. MB-15140
						logging.Errorf(" Invalid opaque Value. Debug info : Res.opaque : %v(%v), Keys %v, Response received %v \n key list %v this key %v", res.Opaque, opaque, len(keys), res, keys, string(res.Body))
						c.setHealthy(false) // who knows what's left to be received
						errch <- fmt.Errorf("Out of Bounds error")
						return
					}

					rv[keys[opaque]] = res
				}

				if res.Opcode == gomemcached.NOOP {
					ok = false

					// notify of the throttle
					if savedErr != nil {
						errch <- savedErr
					}
				}
			}
		}
	}()

	memcachedReqPkt := &gomemcached.MCRequest{
		VBucket: vb,
	}
	if c.replica {
		memcachedReqPkt.Opcode = gomemcached.GET_REPLICA
	} else {
		memcachedReqPkt.Opcode = gomemcached.GET
	}
	err := c.setContext(memcachedReqPkt, context...)
	if err != nil {
		return err
	}

	if len(subPaths) > 0 {
		memcachedReqPkt.Extras, memcachedReqPkt.Body = GetSubDocVal(subPaths, context)
		memcachedReqPkt.Opcode = gomemcached.SUBDOC_MULTI_LOOKUP
	}

	for _, k := range keys { // Start of Get request
		memcachedReqPkt.Key = []byte(k)
		memcachedReqPkt.Opaque = c.opaque

		err := c.Transmit(memcachedReqPkt)
		if err != nil {
			logging.Errorf("Transmit failed in GetBulkAll for key <ud>'%v'</ud>: %v", k, err)
			return err
		}
		c.opaque++
	} // End of Get request

	// finally transmit a NOOP
	err = c.Transmit(&gomemcached.MCRequest{
		Opcode:  gomemcached.NOOP,
		VBucket: vb,
		Opaque:  c.opaque,
	})

	if err != nil {
		logging.Errorf(" Transmit of NOOP failed  %v", err)
		return err
	}
	c.opaque++

	return <-errch
}

func GetSubDocVal(subPaths []string, context []*ClientContext) (extraBuf, valueBuf []byte) {

	// SubdocFlagXattrPath indicates that the path refers to an Xattr rather than the document body.
	flag := uint8(gomemcached.SUBDOC_FLAG_XATTR)
	for i := range context {
		if context[i].DocumentSubDocPaths {
			flag = 0
			break
		}
	}

	var ops []string
	totalBytesLen := 0
	num := 1

	for _, v := range subPaths {
		totalBytesLen = totalBytesLen + len([]byte(v))
		ops = append(ops, v)
		num = num + 1
	}

	if flag != 0 {
		// Xattr retrieval - subdoc multi get
		// Set deleted true only if it is not expiration
		if len(subPaths) != 1 || subPaths[0] != "$document.exptime" {
			extraBuf = append(extraBuf, uint8(0x04))
		}
	}

	valueBuf = make([]byte, num*4+totalBytesLen)

	//opcode for subdoc get
	op := gomemcached.SUBDOC_GET

	// Calculate path total bytes
	// There are 2 ops - get xattrs - both input and $document and get whole doc
	valIter := 0

	for _, v := range ops {
		pathBytes := []byte(v)
		valueBuf[valIter+0] = uint8(op)
		valueBuf[valIter+1] = flag

		// 2 byte key
		binary.BigEndian.PutUint16(valueBuf[valIter+2:], uint16(len(pathBytes)))

		// Then n bytes path
		copy(valueBuf[valIter+4:], pathBytes)
		valIter = valIter + 4 + len(pathBytes)
	}

	return
}

func (c *Client) CreateRangeScan(vb uint16, start []byte, excludeStart bool, end []byte, excludeEnd bool,
	withDocs bool, context ...*ClientContext) (*gomemcached.MCResponse, error) {

	req := &gomemcached.MCRequest{
		Opcode:   gomemcached.CREATE_RANGE_SCAN,
		VBucket:  vb,
		DataType: JSONDataType,
		Opaque:   c.opaque,
	}
	err := c.setContext(req, context...)
	if err != nil {
		return nil, err
	}

	collId := uint32(0)
	xattrs := false
	if len(context) > 0 {
		collId = context[0].CollId
		xattrs = withDocs && context[0].IncludeXATTRs
	}
	req.CollIdLen = 0 // has to be 0 else op is rejected
	r := make(map[string]interface{})
	if excludeStart {
		r["excl_start"] = base64.StdEncoding.EncodeToString(start)
	} else {
		r["start"] = base64.StdEncoding.EncodeToString(start)
	}
	if excludeEnd {
		r["excl_end"] = base64.StdEncoding.EncodeToString(end)
	} else {
		r["end"] = base64.StdEncoding.EncodeToString(end)
	}
	m := make(map[string]interface{})
	if collId == 0 && len(context) > 0 {
		collId = context[0].CollId
	}
	m["collection"] = fmt.Sprintf("%x", collId)
	if !withDocs {
		m["key_only"] = true
	}
	m["range"] = r
	if xattrs {
		m["include_xattrs"] = true
	}
	req.Body, _ = json.Marshal(m)

	c.opaque++
	return c.Send(req)
}

func (c *Client) CreateRandomScan(vb uint16, sampleSize int, withDocs bool, context ...*ClientContext) (
	*gomemcached.MCResponse, error) {

	req := &gomemcached.MCRequest{
		Opcode:   gomemcached.CREATE_RANGE_SCAN,
		VBucket:  vb,
		DataType: JSONDataType,
		Opaque:   c.opaque,
	}
	err := c.setContext(req, context...)
	if err != nil {
		return nil, err
	}

	collId := uint32(0)
	xattrs := false
	if len(context) > 0 {
		collId = context[0].CollId
		xattrs = withDocs && context[0].IncludeXATTRs
	}
	req.CollIdLen = 0 // has to be 0 else op is rejected
	s := make(map[string]interface{})
	seed := uint32(rand.Int())
	if seed == 0 {
		seed = RandomScanSeed
	}
	s["seed"] = seed
	s["samples"] = sampleSize
	m := make(map[string]interface{})
	if collId == 0 && len(context) > 0 {
		collId = context[0].CollId
	}
	m["collection"] = fmt.Sprintf("%x", collId)
	if !withDocs {
		m["key_only"] = true
	}
	m["sampling"] = s
	if xattrs {
		m["include_xattrs"] = true
	}
	req.Body, _ = json.Marshal(m)

	c.opaque++
	return c.Send(req)
}

func (c *Client) ContinueRangeScan(vb uint16, uuid []byte, opaque uint32, items uint32, timeout uint32, maxSize uint32,
	context ...*ClientContext) error {

	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.CONTINUE_RANGE_SCAN,
		VBucket: vb,
		Extras:  make([]byte, 28),
		Opaque:  opaque,
	}
	err := c.setContext(req, context...)
	if err != nil {
		return err
	}
	req.CollIdLen = 0 // has to be 0 else op is rejected
	copy(req.Extras, uuid)
	binary.BigEndian.PutUint32(req.Extras[16:], items)
	binary.BigEndian.PutUint32(req.Extras[20:], timeout)
	binary.BigEndian.PutUint32(req.Extras[24:], maxSize)
	return c.Transmit(req)
}

func (c *Client) CancelRangeScan(vb uint16, uuid []byte, opaque uint32, context ...*ClientContext) (
	*gomemcached.MCResponse, error) {

	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.CANCEL_RANGE_SCAN,
		VBucket: vb,
		Extras:  make([]byte, 16),
		Opaque:  opaque,
	}
	err := c.setContext(req, context...)
	if err != nil {
		return nil, err
	}
	req.CollIdLen = 0 // has to be 0 else op is rejected
	copy(req.Extras, uuid)
	return c.Send(req)
}

func (c *Client) ValidateKey(vb uint16, key string, context ...*ClientContext) (bool, error) {
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.REPLACE,
		VBucket: vb,
		Opaque:  c.opaque,
		Extras:  make([]byte, 8),
		Key:     []byte(key),
		Cas:     0xffffffffffffffff,
	}
	err := c.setContext(req, context...)
	if err != nil {
		return false, err
	}
	resp, err := c.Send(req)
	if resp.Status == gomemcached.KEY_EEXISTS {
		return true, nil
	} else if resp.Status == gomemcached.KEY_ENOENT {
		return false, nil
	}
	return false, err
}

// ObservedStatus is the type reported by the Observe method
type ObservedStatus uint8

// Observation status values.
const (
	ObservedNotPersisted     = ObservedStatus(0x00) // found, not persisted
	ObservedPersisted        = ObservedStatus(0x01) // found, persisted
	ObservedNotFound         = ObservedStatus(0x80) // not found (or a persisted delete)
	ObservedLogicallyDeleted = ObservedStatus(0x81) // pending deletion (not persisted yet)
)

// ObserveResult represents the data obtained by an Observe call
type ObserveResult struct {
	Status          ObservedStatus // Whether the value has been persisted/deleted
	Cas             uint64         // Current value's CAS
	PersistenceTime time.Duration  // Node's average time to persist a value
	ReplicationTime time.Duration  // Node's average time to replicate a value
}

// Observe gets the persistence/replication/CAS state of a key
func (c *Client) Observe(vb uint16, key string) (result ObserveResult, err error) {
	// http://www.couchbase.com/wiki/display/couchbase/Observe
	body := make([]byte, 4+len(key))
	binary.BigEndian.PutUint16(body[0:2], vb)
	binary.BigEndian.PutUint16(body[2:4], uint16(len(key)))
	copy(body[4:4+len(key)], key)

	res, err := c.Send(&gomemcached.MCRequest{
		Opcode:  gomemcached.OBSERVE,
		VBucket: vb,
		Body:    body,
	})
	if err != nil {
		return
	}

	// Parse the response data from the body:
	if len(res.Body) < 2+2+1 {
		err = io.ErrUnexpectedEOF
		return
	}
	outVb := binary.BigEndian.Uint16(res.Body[0:2])
	keyLen := binary.BigEndian.Uint16(res.Body[2:4])
	if len(res.Body) < 2+2+int(keyLen)+1+8 {
		err = io.ErrUnexpectedEOF
		return
	}
	outKey := string(res.Body[4 : 4+keyLen])
	if outVb != vb || outKey != key {
		err = fmt.Errorf("observe returned wrong vbucket/key: %d/%q", outVb, outKey)
		return
	}
	result.Status = ObservedStatus(res.Body[4+keyLen])
	result.Cas = binary.BigEndian.Uint64(res.Body[5+keyLen:])
	// The response reuses the Cas field to store time statistics:
	result.PersistenceTime = time.Duration(res.Cas>>32) * time.Millisecond
	result.ReplicationTime = time.Duration(res.Cas&math.MaxUint32) * time.Millisecond
	return
}

// CheckPersistence checks whether a stored value has been persisted to disk yet.
func (result ObserveResult) CheckPersistence(cas uint64, deletion bool) (persisted bool, overwritten bool) {
	switch {
	case result.Status == ObservedNotFound && deletion:
		persisted = true
	case result.Cas != cas:
		overwritten = true
	case result.Status == ObservedPersisted:
		persisted = true
	}
	return
}

// Sequence number based Observe Implementation
type ObserveSeqResult struct {
	Failover           uint8  // Set to 1 if a failover took place
	VbId               uint16 // vbucket id
	Vbuuid             uint64 // vucket uuid
	LastPersistedSeqNo uint64 // last persisted sequence number
	CurrentSeqNo       uint64 // current sequence number
	OldVbuuid          uint64 // Old bucket vbuuid
	LastSeqNo          uint64 // last sequence number received before failover
}

func (c *Client) ObserveSeq(vb uint16, vbuuid uint64) (result *ObserveSeqResult, err error) {
	// http://www.couchbase.com/wiki/display/couchbase/Observe
	body := make([]byte, 8)
	binary.BigEndian.PutUint64(body[0:8], vbuuid)

	res, err := c.Send(&gomemcached.MCRequest{
		Opcode:  gomemcached.OBSERVE_SEQNO,
		VBucket: vb,
		Body:    body,
		Opaque:  0x01,
	})
	if err != nil {
		return
	}

	if res.Status != gomemcached.SUCCESS {
		return nil, fmt.Errorf(" Observe returned error %v", res.Status)
	}

	// Parse the response data from the body:
	if len(res.Body) < (1 + 2 + 8 + 8 + 8) {
		err = io.ErrUnexpectedEOF
		return
	}

	result = &ObserveSeqResult{}
	result.Failover = res.Body[0]
	result.VbId = binary.BigEndian.Uint16(res.Body[1:3])
	result.Vbuuid = binary.BigEndian.Uint64(res.Body[3:11])
	result.LastPersistedSeqNo = binary.BigEndian.Uint64(res.Body[11:19])
	result.CurrentSeqNo = binary.BigEndian.Uint64(res.Body[19:27])

	// in case of failover processing we can have old vbuuid and the last persisted seq number
	if result.Failover == 1 && len(res.Body) >= (1+2+8+8+8+8+8) {
		result.OldVbuuid = binary.BigEndian.Uint64(res.Body[27:35])
		result.LastSeqNo = binary.BigEndian.Uint64(res.Body[35:43])
	}

	return
}

// CasOp is the type of operation to perform on this CAS loop.
type CasOp uint8

const (
	// CASStore instructs the server to store the new value normally
	CASStore = CasOp(iota)
	// CASQuit instructs the client to stop attempting to CAS, leaving value untouched
	CASQuit
	// CASDelete instructs the server to delete the current value
	CASDelete
)

// User specified termination is returned as an error.
func (c CasOp) Error() string {
	switch c {
	case CASStore:
		return "CAS store"
	case CASQuit:
		return "CAS quit"
	case CASDelete:
		return "CAS delete"
	}
	panic("Unhandled value")
}

//////// CAS TRANSFORM

// CASState tracks the state of CAS over several operations.
//
// This is used directly by CASNext and indirectly by CAS
type CASState struct {
	initialized bool   // false on the first call to CASNext, then true
	Value       []byte // Current value of key; update in place to new value
	Cas         uint64 // Current CAS value of key
	Exists      bool   // Does a value exist for the key? (If not, Value will be nil)
	Err         error  // Error, if any, after CASNext returns false
	resp        *gomemcached.MCResponse
}

// CASNext is a non-callback, loop-based version of CAS method.
//
//	Usage is like this:
//
// var state memcached.CASState
//
//	for client.CASNext(vb, key, exp, &state) {
//	    state.Value = some_mutation(state.Value)
//	}
//
// if state.Err != nil { ... }
func (c *Client) CASNext(vb uint16, k string, exp int, state *CASState) bool {
	if state.initialized {
		if !state.Exists {
			// Adding a new key:
			if state.Value == nil {
				state.Cas = 0
				return false // no-op (delete of non-existent value)
			}
			state.resp, state.Err = c.Add(vb, k, 0, exp, state.Value)
		} else {
			// Updating / deleting a key:
			req := &gomemcached.MCRequest{
				Opcode:  gomemcached.DELETE,
				VBucket: vb,
				Key:     []byte(k),
				Cas:     state.Cas}
			if state.Value != nil {
				req.Opcode = gomemcached.SET
				req.Opaque = 0
				req.Extras = []byte{0, 0, 0, 0, 0, 0, 0, 0}
				req.Body = state.Value

				flags := 0
				binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
			}
			state.resp, state.Err = c.Send(req)
		}

		// If the response status is KEY_EEXISTS or NOT_STORED there's a conflict and we'll need to
		// get the new value (below). Otherwise, we're done (either success or failure) so return:
		if !(state.resp != nil && (state.resp.Status == gomemcached.KEY_EEXISTS ||
			state.resp.Status == gomemcached.NOT_STORED)) {
			state.Cas = state.resp.Cas
			return false // either success or fatal error
		}
	}

	// Initial call, or after a conflict: GET the current value and CAS and return them:
	state.initialized = true
	if state.resp, state.Err = c.Get(vb, k); state.Err == nil {
		state.Exists = true
		state.Value = state.resp.Body
		state.Cas = state.resp.Cas
	} else if state.resp != nil && state.resp.Status == gomemcached.KEY_ENOENT {
		state.Err = nil
		state.Exists = false
		state.Value = nil
		state.Cas = 0
	} else {
		return false // fatal error
	}
	return true // keep going...
}

// CasFunc is type type of function to perform a CAS transform.
//
// Input is the current value, or nil if no value exists.
// The function should return the new value (if any) to set, and the store/quit/delete operation.
type CasFunc func(current []byte) ([]byte, CasOp)

// CAS performs a CAS transform with the given function.
//
// If the value does not exist, a nil current value will be sent to f.
func (c *Client) CAS(vb uint16, k string, f CasFunc,
	initexp int) (*gomemcached.MCResponse, error) {
	var state CASState
	for c.CASNext(vb, k, initexp, &state) {
		newValue, operation := f(state.Value)
		if operation == CASQuit || (operation == CASDelete && state.Value == nil) {
			return nil, operation
		}
		state.Value = newValue
	}
	return state.resp, state.Err
}

// StatValue is one of the stats returned from the Stats method.
type StatValue struct {
	// The stat key
	Key string
	// The stat value
	Val string
}

// Stats requests server-side stats.
//
// Use "" as the stat key for toplevel stats.
func (c *Client) Stats(key string) ([]StatValue, error) {
	rv := make([]StatValue, 0, 128)

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.STAT,
		Key:    []byte(key),
		Opaque: 918494,
	}

	err := c.Transmit(req)
	if err != nil {
		return rv, err
	}

	for {
		res, _, err := getResponse(c.conn, c.hdrBuf)
		if err != nil {
			return rv, err
		}
		k := string(res.Key)
		if k == "" {
			break
		}
		rv = append(rv, StatValue{
			Key: k,
			Val: string(res.Body),
		})
	}
	return rv, nil
}

// Stats requests server-side stats.
//
// Use "" as the stat key for toplevel stats.
func (c *Client) StatsFunc(key string, fn func(key, val []byte)) error {
	req := &gomemcached.MCRequest{
		Opcode: gomemcached.STAT,
		Key:    []byte(key),
		Opaque: 918494,
	}

	err := c.Transmit(req)
	if err != nil {
		return err
	}

	for {
		res, _, err := getResponse(c.conn, c.hdrBuf)
		if err != nil {
			return err
		}
		if len(res.Key) == 0 {
			break
		}
		fn(res.Key, res.Body)
	}
	return nil
}

// StatsMap requests server-side stats similarly to Stats, but returns
// them as a map.
//
// Use "" as the stat key for toplevel stats.
func (c *Client) StatsMap(key string) (map[string]string, error) {
	rv := make(map[string]string)

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.STAT,
		Key:    []byte(key),
		Opaque: 918494,
	}

	err := c.Transmit(req)
	if err != nil {
		return rv, err
	}

	for {
		res, _, err := getResponse(c.conn, c.hdrBuf)
		if err != nil {
			return rv, err
		}
		k := string(res.Key)
		if k == "" {
			break
		}
		rv[k] = string(res.Body)
	}

	return rv, nil
}

// instead of returning a new statsMap, simply populate passed in statsMap, which contains all the keys
// for which stats needs to be retrieved
func (c *Client) StatsMapForSpecifiedStats(key string, statsMap map[string]string) error {

	// clear statsMap
	for key, _ := range statsMap {
		statsMap[key] = ""
	}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.STAT,
		Key:    []byte(key),
		Opaque: 918494,
	}

	err := c.Transmit(req)
	if err != nil {
		return err
	}

	for {
		res, _, err := getResponse(c.conn, c.hdrBuf)
		if err != nil {
			return err
		}
		k := string(res.Key)
		if k == "" {
			break
		}
		if _, ok := statsMap[k]; ok {
			statsMap[k] = string(res.Body)
		}
	}

	return nil
}

// UprGetFailoverLog for given list of vbuckets.
func (mc *Client) UprGetFailoverLog(vb []uint16) (map[uint16]*FailoverLog, error) {

	rq := &gomemcached.MCRequest{
		Opcode: gomemcached.UPR_FAILOVERLOG,
		Opaque: opaqueFailover,
	}

	failoverLogs := make(map[uint16]*FailoverLog)
	for _, vBucket := range vb {
		rq.VBucket = vBucket
		if err := mc.Transmit(rq); err != nil {
			return nil, err
		}
		res, err := mc.Receive()

		if err != nil {
			return nil, fmt.Errorf("failed to receive %s", err.Error())
		} else if res.Opcode != gomemcached.UPR_FAILOVERLOG || res.Status != gomemcached.SUCCESS {
			return nil, fmt.Errorf("unexpected #opcode %v", res.Opcode)
		}

		flog, err := parseFailoverLog(res.Body)
		if err != nil {
			return nil, fmt.Errorf("unable to parse failover logs for vb %d", vb)
		}
		failoverLogs[vBucket] = flog
	}

	return failoverLogs, nil
}

// Hijack exposes the underlying connection from this client.
//
// It also marks the connection as unhealthy since the client will
// have lost control over the connection and can't otherwise verify
// things are in good shape for connection pools.
func (c *Client) Hijack() MemcachedConnection {
	c.setHealthy(false)
	return c.conn
}

func (c *Client) setHealthy(healthy bool) {
	healthyState := UnHealthy
	if healthy {
		healthyState = Healthy
	}
	atomic.StoreUint32(&c.healthy, healthyState)
}

func IfResStatusError(response *gomemcached.MCResponse) bool {
	return response == nil ||
		(response.Status != gomemcached.SUBDOC_BAD_MULTI &&
			response.Status != gomemcached.SUBDOC_PATH_NOT_FOUND &&
			response.Status != gomemcached.SUBDOC_MULTI_PATH_FAILURE_DELETED)
}

func (c *Client) Conn() io.ReadWriteCloser {
	return c.conn
}

// Since the binary request supports only a single collection at a time, it is possible
// that this may be called multiple times in succession by callers to get vbSeqnos for
// multiple collections. Thus, caller could pass in a non-nil map so the gomemcached
// client won't need to allocate new map for each call to prevent too much GC
// NOTE: If collection is enabled and context is not given, KV will still return stats for default collection
func (c *Client) GetAllVbSeqnos(vbSeqnoMap map[uint16]uint64, context ...*ClientContext) (map[uint16]uint64, error) {
	rq := &gomemcached.MCRequest{
		Opcode: gomemcached.GET_ALL_VB_SEQNOS,
		Opaque: opaqueGetSeqno,
	}

	err := c.setVbSeqnoContext(rq, context...)
	if err != nil {
		return vbSeqnoMap, err
	}

	err = c.Transmit(rq)
	if err != nil {
		return vbSeqnoMap, err
	}

	res, err := c.Receive()
	if err != nil {
		return vbSeqnoMap, fmt.Errorf("failed to receive: %v", err)
	}

	vbSeqnosList, err := parseGetSeqnoResp(res.Body)
	if err != nil {
		logging.Errorf("Unable to parse : err: %v\n", err)
		return vbSeqnoMap, err
	}

	if vbSeqnoMap == nil {
		vbSeqnoMap = make(map[uint16]uint64)
	}

	combineMapWithReturnedList(vbSeqnoMap, vbSeqnosList)
	return vbSeqnoMap, nil
}

func combineMapWithReturnedList(vbSeqnoMap map[uint16]uint64, list *VBSeqnos) {
	if list == nil {
		return
	}

	// If the map contains exactly the existing vbs in the list, no need to modify
	needToCleanupMap := true
	if len(vbSeqnoMap) == 0 {
		needToCleanupMap = false
	} else if len(vbSeqnoMap) == len(*list) {
		needToCleanupMap = false
		for _, pair := range *list {
			_, vbExists := vbSeqnoMap[uint16(pair[0])]
			if !vbExists {
				needToCleanupMap = true
				break
			}
		}
	}

	if needToCleanupMap {
		var vbsToDelete []uint16
		for vbInSeqnoMap, _ := range vbSeqnoMap {
			// If a vb in the seqno map doesn't exist in the returned list, need to clean up
			// to ensure returning an accurate result
			found := false
			var vbno uint16
			for _, pair := range *list {
				vbno = uint16(pair[0])
				if vbno == vbInSeqnoMap {
					found = true
					break
				} else if vbno > vbInSeqnoMap {
					// definitely not in the list
					break
				}
			}
			if !found {
				vbsToDelete = append(vbsToDelete, vbInSeqnoMap)
			}
		}

		for _, vbno := range vbsToDelete {
			delete(vbSeqnoMap, vbno)
		}
	}

	// Set the map with data from the list
	for _, pair := range *list {
		vbno := uint16(pair[0])
		seqno := pair[1]
		vbSeqnoMap[vbno] = seqno
	}
}

func (c *Client) GetErrorMap(errMapVersion gomemcached.ErrorMapVersion) (map[string]interface{}, error) {
	if errMapVersion == gomemcached.ErrorMapInvalidVersion {
		return nil, fmt.Errorf("Invalid version used")
	}

	payload := make([]byte, 2, 2)
	binary.BigEndian.PutUint16(payload, uint16(errMapVersion))

	rv, err := c.Send(&gomemcached.MCRequest{
		Opcode: gomemcached.GET_ERROR_MAP,
		Body:   payload,
	})

	if err != nil {
		return nil, err
	}

	errMap := make(map[string]interface{})
	err = json.Unmarshal(rv.Body, &errMap)
	if err != nil {
		return nil, err
	}
	return errMap, nil
}

func (c *Client) EnableDataPool(getter func(uint64) ([]byte, error), doneCb func([]byte)) error {
	if atomic.CompareAndSwapUint32(&c.objPoolEnabled, datapoolDisabled, datapoolInit) {
		c.datapoolGetter = getter
		c.datapoolDone = doneCb
		atomic.CompareAndSwapUint32(&c.objPoolEnabled, datapoolInit, datapoolInitDone)
		return nil
	}
	return fmt.Errorf("Already enabled")
}
