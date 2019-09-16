package mocks

import memcached "github.com/couchbase/gomemcached/client"
import mock "github.com/stretchr/testify/mock"

// UprFeedIface is an autogenerated mock type for the UprFeedIface type
type UprFeedIface struct {
	mock.Mock
}

// ClientAck provides a mock function with given fields: event
func (_m *UprFeedIface) ClientAck(event *memcached.UprEvent) error {
	ret := _m.Called(event)

	var r0 error
	if rf, ok := ret.Get(0).(func(*memcached.UprEvent) error); ok {
		r0 = rf(event)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Close provides a mock function with given fields:
func (_m *UprFeedIface) Close() {
	_m.Called()
}

// CloseStream provides a mock function with given fields: vbno, opaqueMSB
func (_m *UprFeedIface) CloseStream(vbno uint16, opaqueMSB uint16) error {
	ret := _m.Called(vbno, opaqueMSB)

	var r0 error
	if rf, ok := ret.Get(0).(func(uint16, uint16) error); ok {
		r0 = rf(vbno, opaqueMSB)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Closed provides a mock function with given fields:
func (_m *UprFeedIface) Closed() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// GetError provides a mock function with given fields:
func (_m *UprFeedIface) GetError() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetUprEventCh provides a mock function with given fields:
func (_m *UprFeedIface) GetUprEventCh() <-chan *memcached.UprEvent {
	ret := _m.Called()

	var r0 <-chan *memcached.UprEvent
	if rf, ok := ret.Get(0).(func() <-chan *memcached.UprEvent); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan *memcached.UprEvent)
		}
	}

	return r0
}

// GetUprStats provides a mock function with given fields:
func (_m *UprFeedIface) GetUprStats() *memcached.UprStats {
	ret := _m.Called()

	var r0 *memcached.UprStats
	if rf, ok := ret.Get(0).(func() *memcached.UprStats); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*memcached.UprStats)
		}
	}

	return r0
}

// SetPriorityAsync provides a mock function with given fields: p
func (_m *UprFeedIface) SetPriorityAsync(p memcached.PriorityType) error {
	ret := _m.Called(p)

	var r0 error
	if rf, ok := ret.Get(0).(func(memcached.PriorityType) error); ok {
		r0 = rf(p)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StartFeed provides a mock function with given fields:
func (_m *UprFeedIface) StartFeed() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StartFeedWithConfig provides a mock function with given fields: datachan_len
func (_m *UprFeedIface) StartFeedWithConfig(datachan_len int) error {
	ret := _m.Called(datachan_len)

	var r0 error
	if rf, ok := ret.Get(0).(func(int) error); ok {
		r0 = rf(datachan_len)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UprOpen provides a mock function with given fields: name, sequence, bufSize
func (_m *UprFeedIface) UprOpen(name string, sequence uint32, bufSize uint32) error {
	ret := _m.Called(name, sequence, bufSize)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, uint32, uint32) error); ok {
		r0 = rf(name, sequence, bufSize)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UprOpenWithFeatures provides a mock function with given fields: name, sequence, bufSize, features
func (_m *UprFeedIface) UprOpenWithFeatures(name string, sequence uint32, bufSize uint32, features memcached.UprFeatures) (error, memcached.UprFeatures) {
	ret := _m.Called(name, sequence, bufSize, features)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, uint32, uint32, memcached.UprFeatures) error); ok {
		r0 = rf(name, sequence, bufSize, features)
	} else {
		r0 = ret.Error(0)
	}

	var r1 memcached.UprFeatures
	if rf, ok := ret.Get(1).(func(string, uint32, uint32, memcached.UprFeatures) memcached.UprFeatures); ok {
		r1 = rf(name, sequence, bufSize, features)
	} else {
		r1 = ret.Get(1).(memcached.UprFeatures)
	}

	return r0, r1
}

// UprOpenWithXATTR provides a mock function with given fields: name, sequence, bufSize
func (_m *UprFeedIface) UprOpenWithXATTR(name string, sequence uint32, bufSize uint32) error {
	ret := _m.Called(name, sequence, bufSize)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, uint32, uint32) error); ok {
		r0 = rf(name, sequence, bufSize)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UprRequestStream provides a mock function with given fields: vbno, opaqueMSB, flags, vuuid, startSequence, endSequence, snapStart, snapEnd
func (_m *UprFeedIface) UprRequestStream(vbno uint16, opaqueMSB uint16, flags uint32, vuuid uint64, startSequence uint64, endSequence uint64, snapStart uint64, snapEnd uint64) error {
	ret := _m.Called(vbno, opaqueMSB, flags, vuuid, startSequence, endSequence, snapStart, snapEnd)

	var r0 error
	if rf, ok := ret.Get(0).(func(uint16, uint16, uint32, uint64, uint64, uint64, uint64, uint64) error); ok {
		r0 = rf(vbno, opaqueMSB, flags, vuuid, startSequence, endSequence, snapStart, snapEnd)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
