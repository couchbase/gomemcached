// Code generated by mockery. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// CollectionCreateEvent is an autogenerated mock type for the CollectionCreateEvent type
type CollectionCreateEvent struct {
	mock.Mock
}

type CollectionCreateEvent_Expecter struct {
	mock *mock.Mock
}

func (_m *CollectionCreateEvent) EXPECT() *CollectionCreateEvent_Expecter {
	return &CollectionCreateEvent_Expecter{mock: &_m.Mock}
}

// GetCollectionId provides a mock function with given fields:
func (_m *CollectionCreateEvent) GetCollectionId() (uint32, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetCollectionId")
	}

	var r0 uint32
	var r1 error
	if rf, ok := ret.Get(0).(func() (uint32, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() uint32); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint32)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionCreateEvent_GetCollectionId_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetCollectionId'
type CollectionCreateEvent_GetCollectionId_Call struct {
	*mock.Call
}

// GetCollectionId is a helper method to define mock.On call
func (_e *CollectionCreateEvent_Expecter) GetCollectionId() *CollectionCreateEvent_GetCollectionId_Call {
	return &CollectionCreateEvent_GetCollectionId_Call{Call: _e.mock.On("GetCollectionId")}
}

func (_c *CollectionCreateEvent_GetCollectionId_Call) Run(run func()) *CollectionCreateEvent_GetCollectionId_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CollectionCreateEvent_GetCollectionId_Call) Return(_a0 uint32, _a1 error) *CollectionCreateEvent_GetCollectionId_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionCreateEvent_GetCollectionId_Call) RunAndReturn(run func() (uint32, error)) *CollectionCreateEvent_GetCollectionId_Call {
	_c.Call.Return(run)
	return _c
}

// GetManifestId provides a mock function with given fields:
func (_m *CollectionCreateEvent) GetManifestId() (uint64, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetManifestId")
	}

	var r0 uint64
	var r1 error
	if rf, ok := ret.Get(0).(func() (uint64, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() uint64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint64)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionCreateEvent_GetManifestId_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetManifestId'
type CollectionCreateEvent_GetManifestId_Call struct {
	*mock.Call
}

// GetManifestId is a helper method to define mock.On call
func (_e *CollectionCreateEvent_Expecter) GetManifestId() *CollectionCreateEvent_GetManifestId_Call {
	return &CollectionCreateEvent_GetManifestId_Call{Call: _e.mock.On("GetManifestId")}
}

func (_c *CollectionCreateEvent_GetManifestId_Call) Run(run func()) *CollectionCreateEvent_GetManifestId_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CollectionCreateEvent_GetManifestId_Call) Return(_a0 uint64, _a1 error) *CollectionCreateEvent_GetManifestId_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionCreateEvent_GetManifestId_Call) RunAndReturn(run func() (uint64, error)) *CollectionCreateEvent_GetManifestId_Call {
	_c.Call.Return(run)
	return _c
}

// GetMaxTTL provides a mock function with given fields:
func (_m *CollectionCreateEvent) GetMaxTTL() (uint32, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetMaxTTL")
	}

	var r0 uint32
	var r1 error
	if rf, ok := ret.Get(0).(func() (uint32, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() uint32); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint32)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionCreateEvent_GetMaxTTL_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetMaxTTL'
type CollectionCreateEvent_GetMaxTTL_Call struct {
	*mock.Call
}

// GetMaxTTL is a helper method to define mock.On call
func (_e *CollectionCreateEvent_Expecter) GetMaxTTL() *CollectionCreateEvent_GetMaxTTL_Call {
	return &CollectionCreateEvent_GetMaxTTL_Call{Call: _e.mock.On("GetMaxTTL")}
}

func (_c *CollectionCreateEvent_GetMaxTTL_Call) Run(run func()) *CollectionCreateEvent_GetMaxTTL_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CollectionCreateEvent_GetMaxTTL_Call) Return(_a0 uint32, _a1 error) *CollectionCreateEvent_GetMaxTTL_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionCreateEvent_GetMaxTTL_Call) RunAndReturn(run func() (uint32, error)) *CollectionCreateEvent_GetMaxTTL_Call {
	_c.Call.Return(run)
	return _c
}

// GetScopeId provides a mock function with given fields:
func (_m *CollectionCreateEvent) GetScopeId() (uint32, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetScopeId")
	}

	var r0 uint32
	var r1 error
	if rf, ok := ret.Get(0).(func() (uint32, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() uint32); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint32)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionCreateEvent_GetScopeId_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetScopeId'
type CollectionCreateEvent_GetScopeId_Call struct {
	*mock.Call
}

// GetScopeId is a helper method to define mock.On call
func (_e *CollectionCreateEvent_Expecter) GetScopeId() *CollectionCreateEvent_GetScopeId_Call {
	return &CollectionCreateEvent_GetScopeId_Call{Call: _e.mock.On("GetScopeId")}
}

func (_c *CollectionCreateEvent_GetScopeId_Call) Run(run func()) *CollectionCreateEvent_GetScopeId_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CollectionCreateEvent_GetScopeId_Call) Return(_a0 uint32, _a1 error) *CollectionCreateEvent_GetScopeId_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionCreateEvent_GetScopeId_Call) RunAndReturn(run func() (uint32, error)) *CollectionCreateEvent_GetScopeId_Call {
	_c.Call.Return(run)
	return _c
}

// GetSystemEventName provides a mock function with given fields:
func (_m *CollectionCreateEvent) GetSystemEventName() (string, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetSystemEventName")
	}

	var r0 string
	var r1 error
	if rf, ok := ret.Get(0).(func() (string, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CollectionCreateEvent_GetSystemEventName_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSystemEventName'
type CollectionCreateEvent_GetSystemEventName_Call struct {
	*mock.Call
}

// GetSystemEventName is a helper method to define mock.On call
func (_e *CollectionCreateEvent_Expecter) GetSystemEventName() *CollectionCreateEvent_GetSystemEventName_Call {
	return &CollectionCreateEvent_GetSystemEventName_Call{Call: _e.mock.On("GetSystemEventName")}
}

func (_c *CollectionCreateEvent_GetSystemEventName_Call) Run(run func()) *CollectionCreateEvent_GetSystemEventName_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *CollectionCreateEvent_GetSystemEventName_Call) Return(_a0 string, _a1 error) *CollectionCreateEvent_GetSystemEventName_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *CollectionCreateEvent_GetSystemEventName_Call) RunAndReturn(run func() (string, error)) *CollectionCreateEvent_GetSystemEventName_Call {
	_c.Call.Return(run)
	return _c
}

// NewCollectionCreateEvent creates a new instance of CollectionCreateEvent. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewCollectionCreateEvent(t interface {
	mock.TestingT
	Cleanup(func())
}) *CollectionCreateEvent {
	mock := &CollectionCreateEvent{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
