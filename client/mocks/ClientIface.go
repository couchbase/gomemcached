// Code generated by mockery v1.0.0
package mocks

import gomemcached "github.com/couchbase/gomemcached"
import io "io"
import memcached "github.com/couchbase/gomemcached/client"
import mock "github.com/stretchr/testify/mock"
import time "time"

// ClientIface is an autogenerated mock type for the ClientIface type
type ClientIface struct {
	mock.Mock
}

// Add provides a mock function with given fields: vb, key, flags, exp, body
func (_m *ClientIface) Add(vb uint16, key string, flags int, exp int, body []byte) (*gomemcached.MCResponse, error) {
	ret := _m.Called(vb, key, flags, exp, body)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(uint16, string, int, int, []byte) *gomemcached.MCResponse); ok {
		r0 = rf(vb, key, flags, exp, body)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, string, int, int, []byte) error); ok {
		r1 = rf(vb, key, flags, exp, body)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Append provides a mock function with given fields: vb, key, data
func (_m *ClientIface) Append(vb uint16, key string, data []byte) (*gomemcached.MCResponse, error) {
	ret := _m.Called(vb, key, data)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(uint16, string, []byte) *gomemcached.MCResponse); ok {
		r0 = rf(vb, key, data)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, string, []byte) error); ok {
		r1 = rf(vb, key, data)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Auth provides a mock function with given fields: user, pass
func (_m *ClientIface) Auth(user string, pass string) (*gomemcached.MCResponse, error) {
	ret := _m.Called(user, pass)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(string, string) *gomemcached.MCResponse); ok {
		r0 = rf(user, pass)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(user, pass)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AuthList provides a mock function with given fields:
func (_m *ClientIface) AuthList() (*gomemcached.MCResponse, error) {
	ret := _m.Called()

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func() *gomemcached.MCResponse); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AuthPlain provides a mock function with given fields: user, pass
func (_m *ClientIface) AuthPlain(user string, pass string) (*gomemcached.MCResponse, error) {
	ret := _m.Called(user, pass)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(string, string) *gomemcached.MCResponse); ok {
		r0 = rf(user, pass)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(user, pass)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AuthScramSha provides a mock function with given fields: user, pass
func (_m *ClientIface) AuthScramSha(user string, pass string) (*gomemcached.MCResponse, error) {
	ret := _m.Called(user, pass)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(string, string) *gomemcached.MCResponse); ok {
		r0 = rf(user, pass)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(user, pass)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CAS provides a mock function with given fields: vb, k, f, initexp
func (_m *ClientIface) CAS(vb uint16, k string, f memcached.CasFunc, initexp int) (*gomemcached.MCResponse, error) {
	ret := _m.Called(vb, k, f, initexp)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(uint16, string, memcached.CasFunc, int) *gomemcached.MCResponse); ok {
		r0 = rf(vb, k, f, initexp)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, string, memcached.CasFunc, int) error); ok {
		r1 = rf(vb, k, f, initexp)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CASNext provides a mock function with given fields: vb, k, exp, state
func (_m *ClientIface) CASNext(vb uint16, k string, exp int, state *memcached.CASState) bool {
	ret := _m.Called(vb, k, exp, state)

	var r0 bool
	if rf, ok := ret.Get(0).(func(uint16, string, int, *memcached.CASState) bool); ok {
		r0 = rf(vb, k, exp, state)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// Close provides a mock function with given fields:
func (_m *ClientIface) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CollectionsGetCID provides a mock function with given fields: scope, collection
func (_m *ClientIface) CollectionsGetCID(scope string, collection string) (*gomemcached.MCResponse, error) {
	ret := _m.Called(scope, collection)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(string, string) *gomemcached.MCResponse); ok {
		r0 = rf(scope, collection)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(scope, collection)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Decr provides a mock function with given fields: vb, key, amt, def, exp
func (_m *ClientIface) Decr(vb uint16, key string, amt uint64, def uint64, exp int) (uint64, error) {
	ret := _m.Called(vb, key, amt, def, exp)

	var r0 uint64
	if rf, ok := ret.Get(0).(func(uint16, string, uint64, uint64, int) uint64); ok {
		r0 = rf(vb, key, amt, def, exp)
	} else {
		r0 = ret.Get(0).(uint64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, string, uint64, uint64, int) error); ok {
		r1 = rf(vb, key, amt, def, exp)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Del provides a mock function with given fields: vb, key
func (_m *ClientIface) Del(vb uint16, key string) (*gomemcached.MCResponse, error) {
	ret := _m.Called(vb, key)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(uint16, string) *gomemcached.MCResponse); ok {
		r0 = rf(vb, key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, string) error); ok {
		r1 = rf(vb, key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// EnableMutationToken provides a mock function with given fields:
func (_m *ClientIface) EnableMutationToken() (*gomemcached.MCResponse, error) {
	ret := _m.Called()

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func() *gomemcached.MCResponse); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Get provides a mock function with given fields: vb, key
func (_m *ClientIface) Get(vb uint16, key string) (*gomemcached.MCResponse, error) {
	ret := _m.Called(vb, key)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(uint16, string) *gomemcached.MCResponse); ok {
		r0 = rf(vb, key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, string) error); ok {
		r1 = rf(vb, key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAndTouch provides a mock function with given fields: vb, key, exp
func (_m *ClientIface) GetAndTouch(vb uint16, key string, exp int) (*gomemcached.MCResponse, error) {
	ret := _m.Called(vb, key, exp)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(uint16, string, int) *gomemcached.MCResponse); ok {
		r0 = rf(vb, key, exp)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, string, int) error); ok {
		r1 = rf(vb, key, exp)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetBulk provides a mock function with given fields: vb, keys, rv, subPaths
func (_m *ClientIface) GetBulk(vb uint16, keys []string, rv map[string]*gomemcached.MCResponse, subPaths []string) error {
	ret := _m.Called(vb, keys, rv, subPaths)

	var r0 error
	if rf, ok := ret.Get(0).(func(uint16, []string, map[string]*gomemcached.MCResponse, []string) error); ok {
		r0 = rf(vb, keys, rv, subPaths)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetCollectionsManifest provides a mock function with given fields:
func (_m *ClientIface) GetCollectionsManifest() (*gomemcached.MCResponse, error) {
	ret := _m.Called()

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func() *gomemcached.MCResponse); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetFromCollection provides a mock function with given fields: vb, cid, key
func (_m *ClientIface) GetFromCollection(vb uint16, cid uint32, key string) (*gomemcached.MCResponse, error) {
	ret := _m.Called(vb, cid, key)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(uint16, uint32, string) *gomemcached.MCResponse); ok {
		r0 = rf(vb, cid, key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, uint32, string) error); ok {
		r1 = rf(vb, cid, key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetMeta provides a mock function with given fields: vb, key
func (_m *ClientIface) GetMeta(vb uint16, key string) (*gomemcached.MCResponse, error) {
	ret := _m.Called(vb, key)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(uint16, string) *gomemcached.MCResponse); ok {
		r0 = rf(vb, key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, string) error); ok {
		r1 = rf(vb, key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetRandomDoc provides a mock function with given fields:
func (_m *ClientIface) GetRandomDoc() (*gomemcached.MCResponse, error) {
	ret := _m.Called()

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func() *gomemcached.MCResponse); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetSubdoc provides a mock function with given fields: vb, key, subPaths
func (_m *ClientIface) GetSubdoc(vb uint16, key string, subPaths []string) (*gomemcached.MCResponse, error) {
	ret := _m.Called(vb, key, subPaths)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(uint16, string, []string) *gomemcached.MCResponse); ok {
		r0 = rf(vb, key, subPaths)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, string, []string) error); ok {
		r1 = rf(vb, key, subPaths)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Hijack provides a mock function with given fields:
func (_m *ClientIface) Hijack() io.ReadWriteCloser {
	ret := _m.Called()

	var r0 io.ReadWriteCloser
	if rf, ok := ret.Get(0).(func() io.ReadWriteCloser); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(io.ReadWriteCloser)
		}
	}

	return r0
}

// Incr provides a mock function with given fields: vb, key, amt, def, exp
func (_m *ClientIface) Incr(vb uint16, key string, amt uint64, def uint64, exp int) (uint64, error) {
	ret := _m.Called(vb, key, amt, def, exp)

	var r0 uint64
	if rf, ok := ret.Get(0).(func(uint16, string, uint64, uint64, int) uint64); ok {
		r0 = rf(vb, key, amt, def, exp)
	} else {
		r0 = ret.Get(0).(uint64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, string, uint64, uint64, int) error); ok {
		r1 = rf(vb, key, amt, def, exp)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewUprFeed provides a mock function with given fields:
func (_m *ClientIface) NewUprFeed() (*memcached.UprFeed, error) {
	ret := _m.Called()

	var r0 *memcached.UprFeed
	if rf, ok := ret.Get(0).(func() *memcached.UprFeed); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*memcached.UprFeed)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewUprFeedIface provides a mock function with given fields:
func (_m *ClientIface) NewUprFeedIface() (memcached.UprFeedIface, error) {
	ret := _m.Called()

	var r0 memcached.UprFeedIface
	if rf, ok := ret.Get(0).(func() memcached.UprFeedIface); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(memcached.UprFeedIface)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewUprFeedWithConfig provides a mock function with given fields: ackByClient
func (_m *ClientIface) NewUprFeedWithConfig(ackByClient bool) (*memcached.UprFeed, error) {
	ret := _m.Called(ackByClient)

	var r0 *memcached.UprFeed
	if rf, ok := ret.Get(0).(func(bool) *memcached.UprFeed); ok {
		r0 = rf(ackByClient)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*memcached.UprFeed)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(bool) error); ok {
		r1 = rf(ackByClient)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewUprFeedWithConfigIface provides a mock function with given fields: ackByClient
func (_m *ClientIface) NewUprFeedWithConfigIface(ackByClient bool) (memcached.UprFeedIface, error) {
	ret := _m.Called(ackByClient)

	var r0 memcached.UprFeedIface
	if rf, ok := ret.Get(0).(func(bool) memcached.UprFeedIface); ok {
		r0 = rf(ackByClient)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(memcached.UprFeedIface)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(bool) error); ok {
		r1 = rf(ackByClient)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Observe provides a mock function with given fields: vb, key
func (_m *ClientIface) Observe(vb uint16, key string) (memcached.ObserveResult, error) {
	ret := _m.Called(vb, key)

	var r0 memcached.ObserveResult
	if rf, ok := ret.Get(0).(func(uint16, string) memcached.ObserveResult); ok {
		r0 = rf(vb, key)
	} else {
		r0 = ret.Get(0).(memcached.ObserveResult)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, string) error); ok {
		r1 = rf(vb, key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ObserveSeq provides a mock function with given fields: vb, vbuuid
func (_m *ClientIface) ObserveSeq(vb uint16, vbuuid uint64) (*memcached.ObserveSeqResult, error) {
	ret := _m.Called(vb, vbuuid)

	var r0 *memcached.ObserveSeqResult
	if rf, ok := ret.Get(0).(func(uint16, uint64) *memcached.ObserveSeqResult); ok {
		r0 = rf(vb, vbuuid)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*memcached.ObserveSeqResult)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, uint64) error); ok {
		r1 = rf(vb, vbuuid)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Receive provides a mock function with given fields:
func (_m *ClientIface) Receive() (*gomemcached.MCResponse, error) {
	ret := _m.Called()

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func() *gomemcached.MCResponse); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReceiveWithDeadline provides a mock function with given fields: deadline
func (_m *ClientIface) ReceiveWithDeadline(deadline time.Time) (*gomemcached.MCResponse, error) {
	ret := _m.Called(deadline)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(time.Time) *gomemcached.MCResponse); ok {
		r0 = rf(deadline)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(time.Time) error); ok {
		r1 = rf(deadline)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SelectBucket provides a mock function with given fields: bucket
func (_m *ClientIface) SelectBucket(bucket string) (*gomemcached.MCResponse, error) {
	ret := _m.Called(bucket)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(string) *gomemcached.MCResponse); ok {
		r0 = rf(bucket)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(bucket)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Send provides a mock function with given fields: req
func (_m *ClientIface) Send(req *gomemcached.MCRequest) (*gomemcached.MCResponse, error) {
	ret := _m.Called(req)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(*gomemcached.MCRequest) *gomemcached.MCResponse); ok {
		r0 = rf(req)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*gomemcached.MCRequest) error); ok {
		r1 = rf(req)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Set provides a mock function with given fields: vb, key, flags, exp, body
func (_m *ClientIface) Set(vb uint16, key string, flags int, exp int, body []byte) (*gomemcached.MCResponse, error) {
	ret := _m.Called(vb, key, flags, exp, body)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(uint16, string, int, int, []byte) *gomemcached.MCResponse); ok {
		r0 = rf(vb, key, flags, exp, body)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, string, int, int, []byte) error); ok {
		r1 = rf(vb, key, flags, exp, body)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SetCas provides a mock function with given fields: vb, key, flags, exp, cas, body
func (_m *ClientIface) SetCas(vb uint16, key string, flags int, exp int, cas uint64, body []byte) (*gomemcached.MCResponse, error) {
	ret := _m.Called(vb, key, flags, exp, cas, body)

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(uint16, string, int, int, uint64, []byte) *gomemcached.MCResponse); ok {
		r0 = rf(vb, key, flags, exp, cas, body)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint16, string, int, int, uint64, []byte) error); ok {
		r1 = rf(vb, key, flags, exp, cas, body)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SetDeadline provides a mock function with given fields: t
func (_m *ClientIface) SetDeadline(t time.Time) {
	_m.Called(t)
}

// SetKeepAliveOptions provides a mock function with given fields: interval
func (_m *ClientIface) SetKeepAliveOptions(interval time.Duration) {
	_m.Called(interval)
}

// SetReadDeadline provides a mock function with given fields: t
func (_m *ClientIface) SetReadDeadline(t time.Time) {
	_m.Called(t)
}

// Stats provides a mock function with given fields: key
func (_m *ClientIface) Stats(key string) ([]memcached.StatValue, error) {
	ret := _m.Called(key)

	var r0 []memcached.StatValue
	if rf, ok := ret.Get(0).(func(string) []memcached.StatValue); ok {
		r0 = rf(key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]memcached.StatValue)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// StatsMap provides a mock function with given fields: key
func (_m *ClientIface) StatsMap(key string) (map[string]string, error) {
	ret := _m.Called(key)

	var r0 map[string]string
	if rf, ok := ret.Get(0).(func(string) map[string]string); ok {
		r0 = rf(key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]string)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// StatsMapForSpecifiedStats provides a mock function with given fields: key, statsMap
func (_m *ClientIface) StatsMapForSpecifiedStats(key string, statsMap map[string]string) error {
	ret := _m.Called(key, statsMap)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, map[string]string) error); ok {
		r0 = rf(key, statsMap)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Transmit provides a mock function with given fields: req
func (_m *ClientIface) Transmit(req *gomemcached.MCRequest) error {
	ret := _m.Called(req)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gomemcached.MCRequest) error); ok {
		r0 = rf(req)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// TransmitResponse provides a mock function with given fields: res
func (_m *ClientIface) TransmitResponse(res *gomemcached.MCResponse) error {
	ret := _m.Called(res)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gomemcached.MCResponse) error); ok {
		r0 = rf(res)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// TransmitWithDeadline provides a mock function with given fields: req, deadline
func (_m *ClientIface) TransmitWithDeadline(req *gomemcached.MCRequest, deadline time.Time) error {
	ret := _m.Called(req, deadline)

	var r0 error
	if rf, ok := ret.Get(0).(func(*gomemcached.MCRequest, time.Time) error); ok {
		r0 = rf(req, deadline)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UprGetFailoverLog provides a mock function with given fields: vb
func (_m *ClientIface) UprGetFailoverLog(vb []uint16) (map[uint16]*memcached.FailoverLog, error) {
	ret := _m.Called(vb)

	var r0 map[uint16]*memcached.FailoverLog
	if rf, ok := ret.Get(0).(func([]uint16) map[uint16]*memcached.FailoverLog); ok {
		r0 = rf(vb)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[uint16]*memcached.FailoverLog)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func([]uint16) error); ok {
		r1 = rf(vb)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
