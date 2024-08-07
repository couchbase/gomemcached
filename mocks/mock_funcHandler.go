// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	io "io"

	gomemcached "github.com/couchbase/gomemcached"

	mock "github.com/stretchr/testify/mock"
)

// funcHandler is an autogenerated mock type for the funcHandler type
type funcHandler struct {
	mock.Mock
}

type funcHandler_Expecter struct {
	mock *mock.Mock
}

func (_m *funcHandler) EXPECT() *funcHandler_Expecter {
	return &funcHandler_Expecter{mock: &_m.Mock}
}

// Execute provides a mock function with given fields: _a0, _a1
func (_m *funcHandler) Execute(_a0 io.Writer, _a1 *gomemcached.MCRequest) *gomemcached.MCResponse {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for Execute")
	}

	var r0 *gomemcached.MCResponse
	if rf, ok := ret.Get(0).(func(io.Writer, *gomemcached.MCRequest) *gomemcached.MCResponse); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*gomemcached.MCResponse)
		}
	}

	return r0
}

// funcHandler_Execute_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Execute'
type funcHandler_Execute_Call struct {
	*mock.Call
}

// Execute is a helper method to define mock.On call
//   - _a0 io.Writer
//   - _a1 *gomemcached.MCRequest
func (_e *funcHandler_Expecter) Execute(_a0 interface{}, _a1 interface{}) *funcHandler_Execute_Call {
	return &funcHandler_Execute_Call{Call: _e.mock.On("Execute", _a0, _a1)}
}

func (_c *funcHandler_Execute_Call) Run(run func(_a0 io.Writer, _a1 *gomemcached.MCRequest)) *funcHandler_Execute_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(io.Writer), args[1].(*gomemcached.MCRequest))
	})
	return _c
}

func (_c *funcHandler_Execute_Call) Return(_a0 *gomemcached.MCResponse) *funcHandler_Execute_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *funcHandler_Execute_Call) RunAndReturn(run func(io.Writer, *gomemcached.MCRequest) *gomemcached.MCResponse) *funcHandler_Execute_Call {
	_c.Call.Return(run)
	return _c
}

// newFuncHandler creates a new instance of funcHandler. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newFuncHandler(t interface {
	mock.TestingT
	Cleanup(func())
}) *funcHandler {
	mock := &funcHandler{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
