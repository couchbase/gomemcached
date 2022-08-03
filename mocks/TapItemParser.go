// Code generated by mockery v2.14.0. DO NOT EDIT.

package mocks

import (
	io "io"

	mock "github.com/stretchr/testify/mock"
)

// TapItemParser is an autogenerated mock type for the TapItemParser type
type TapItemParser struct {
	mock.Mock
}

// Execute provides a mock function with given fields: _a0
func (_m *TapItemParser) Execute(_a0 io.Reader) (interface{}, error) {
	ret := _m.Called(_a0)

	var r0 interface{}
	if rf, ok := ret.Get(0).(func(io.Reader) interface{}); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(io.Reader) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewTapItemParser interface {
	mock.TestingT
	Cleanup(func())
}

// NewTapItemParser creates a new instance of TapItemParser. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewTapItemParser(t mockConstructorTestingTNewTapItemParser) *TapItemParser {
	mock := &TapItemParser{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
