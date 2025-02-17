// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package systemevents

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Scope struct {
	_tab flatbuffers.Table
}

func GetRootAsScope(buf []byte, offset flatbuffers.UOffsetT) *Scope {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Scope{}
	x.Init(buf, n+offset)
	return x
}

func FinishScopeBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsScope(buf []byte, offset flatbuffers.UOffsetT) *Scope {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Scope{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedScopeBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *Scope) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Scope) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Scope) Uid() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Scope) MutateUid(n uint64) bool {
	return rcv._tab.MutateUint64Slot(4, n)
}

func (rcv *Scope) ScopeId() uint32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Scope) MutateScopeId(n uint32) bool {
	return rcv._tab.MutateUint32Slot(6, n)
}

func (rcv *Scope) Name() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func ScopeStart(builder *flatbuffers.Builder) {
	builder.StartObject(3)
}
func ScopeAddUid(builder *flatbuffers.Builder, uid uint64) {
	builder.PrependUint64Slot(0, uid, 0)
}
func ScopeAddScopeId(builder *flatbuffers.Builder, scopeId uint32) {
	builder.PrependUint32Slot(1, scopeId, 0)
}
func ScopeAddName(builder *flatbuffers.Builder, name flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(name), 0)
}
func ScopeEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
