// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	time "time"

	mock "github.com/stretchr/testify/mock"
)

// MemcachedConnection is an autogenerated mock type for the MemcachedConnection type
type MemcachedConnection struct {
	mock.Mock
}

type MemcachedConnection_Expecter struct {
	mock *mock.Mock
}

func (_m *MemcachedConnection) EXPECT() *MemcachedConnection_Expecter {
	return &MemcachedConnection_Expecter{mock: &_m.Mock}
}

// Close provides a mock function with given fields:
func (_m *MemcachedConnection) Close() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Close")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MemcachedConnection_Close_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Close'
type MemcachedConnection_Close_Call struct {
	*mock.Call
}

// Close is a helper method to define mock.On call
func (_e *MemcachedConnection_Expecter) Close() *MemcachedConnection_Close_Call {
	return &MemcachedConnection_Close_Call{Call: _e.mock.On("Close")}
}

func (_c *MemcachedConnection_Close_Call) Run(run func()) *MemcachedConnection_Close_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MemcachedConnection_Close_Call) Return(_a0 error) *MemcachedConnection_Close_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MemcachedConnection_Close_Call) RunAndReturn(run func() error) *MemcachedConnection_Close_Call {
	_c.Call.Return(run)
	return _c
}

// Read provides a mock function with given fields: p
func (_m *MemcachedConnection) Read(p []byte) (int, error) {
	ret := _m.Called(p)

	if len(ret) == 0 {
		panic("no return value specified for Read")
	}

	var r0 int
	var r1 error
	if rf, ok := ret.Get(0).(func([]byte) (int, error)); ok {
		return rf(p)
	}
	if rf, ok := ret.Get(0).(func([]byte) int); ok {
		r0 = rf(p)
	} else {
		r0 = ret.Get(0).(int)
	}

	if rf, ok := ret.Get(1).(func([]byte) error); ok {
		r1 = rf(p)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MemcachedConnection_Read_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Read'
type MemcachedConnection_Read_Call struct {
	*mock.Call
}

// Read is a helper method to define mock.On call
//   - p []byte
func (_e *MemcachedConnection_Expecter) Read(p interface{}) *MemcachedConnection_Read_Call {
	return &MemcachedConnection_Read_Call{Call: _e.mock.On("Read", p)}
}

func (_c *MemcachedConnection_Read_Call) Run(run func(p []byte)) *MemcachedConnection_Read_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]byte))
	})
	return _c
}

func (_c *MemcachedConnection_Read_Call) Return(n int, err error) *MemcachedConnection_Read_Call {
	_c.Call.Return(n, err)
	return _c
}

func (_c *MemcachedConnection_Read_Call) RunAndReturn(run func([]byte) (int, error)) *MemcachedConnection_Read_Call {
	_c.Call.Return(run)
	return _c
}

// SetDeadline provides a mock function with given fields: _a0
func (_m *MemcachedConnection) SetDeadline(_a0 time.Time) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for SetDeadline")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(time.Time) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MemcachedConnection_SetDeadline_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetDeadline'
type MemcachedConnection_SetDeadline_Call struct {
	*mock.Call
}

// SetDeadline is a helper method to define mock.On call
//   - _a0 time.Time
func (_e *MemcachedConnection_Expecter) SetDeadline(_a0 interface{}) *MemcachedConnection_SetDeadline_Call {
	return &MemcachedConnection_SetDeadline_Call{Call: _e.mock.On("SetDeadline", _a0)}
}

func (_c *MemcachedConnection_SetDeadline_Call) Run(run func(_a0 time.Time)) *MemcachedConnection_SetDeadline_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(time.Time))
	})
	return _c
}

func (_c *MemcachedConnection_SetDeadline_Call) Return(_a0 error) *MemcachedConnection_SetDeadline_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MemcachedConnection_SetDeadline_Call) RunAndReturn(run func(time.Time) error) *MemcachedConnection_SetDeadline_Call {
	_c.Call.Return(run)
	return _c
}

// SetReadDeadline provides a mock function with given fields: _a0
func (_m *MemcachedConnection) SetReadDeadline(_a0 time.Time) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for SetReadDeadline")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(time.Time) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MemcachedConnection_SetReadDeadline_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetReadDeadline'
type MemcachedConnection_SetReadDeadline_Call struct {
	*mock.Call
}

// SetReadDeadline is a helper method to define mock.On call
//   - _a0 time.Time
func (_e *MemcachedConnection_Expecter) SetReadDeadline(_a0 interface{}) *MemcachedConnection_SetReadDeadline_Call {
	return &MemcachedConnection_SetReadDeadline_Call{Call: _e.mock.On("SetReadDeadline", _a0)}
}

func (_c *MemcachedConnection_SetReadDeadline_Call) Run(run func(_a0 time.Time)) *MemcachedConnection_SetReadDeadline_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(time.Time))
	})
	return _c
}

func (_c *MemcachedConnection_SetReadDeadline_Call) Return(_a0 error) *MemcachedConnection_SetReadDeadline_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MemcachedConnection_SetReadDeadline_Call) RunAndReturn(run func(time.Time) error) *MemcachedConnection_SetReadDeadline_Call {
	_c.Call.Return(run)
	return _c
}

// Write provides a mock function with given fields: p
func (_m *MemcachedConnection) Write(p []byte) (int, error) {
	ret := _m.Called(p)

	if len(ret) == 0 {
		panic("no return value specified for Write")
	}

	var r0 int
	var r1 error
	if rf, ok := ret.Get(0).(func([]byte) (int, error)); ok {
		return rf(p)
	}
	if rf, ok := ret.Get(0).(func([]byte) int); ok {
		r0 = rf(p)
	} else {
		r0 = ret.Get(0).(int)
	}

	if rf, ok := ret.Get(1).(func([]byte) error); ok {
		r1 = rf(p)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MemcachedConnection_Write_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Write'
type MemcachedConnection_Write_Call struct {
	*mock.Call
}

// Write is a helper method to define mock.On call
//   - p []byte
func (_e *MemcachedConnection_Expecter) Write(p interface{}) *MemcachedConnection_Write_Call {
	return &MemcachedConnection_Write_Call{Call: _e.mock.On("Write", p)}
}

func (_c *MemcachedConnection_Write_Call) Run(run func(p []byte)) *MemcachedConnection_Write_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]byte))
	})
	return _c
}

func (_c *MemcachedConnection_Write_Call) Return(n int, err error) *MemcachedConnection_Write_Call {
	_c.Call.Return(n, err)
	return _c
}

func (_c *MemcachedConnection_Write_Call) RunAndReturn(run func([]byte) (int, error)) *MemcachedConnection_Write_Call {
	_c.Call.Return(run)
	return _c
}

// NewMemcachedConnection creates a new instance of MemcachedConnection. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMemcachedConnection(t interface {
	mock.TestingT
	Cleanup(func())
}) *MemcachedConnection {
	mock := &MemcachedConnection{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
