package memcached

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/couchbase/gomemcached"
	"github.com/couchbase/gomemcached/internal/flatbuffers/systemevents"
)

type SystemEventType int

const InvalidSysEvent SystemEventType = -1

const (
	CollectionCreate  SystemEventType = 0
	CollectionDrop    SystemEventType = iota
	CollectionFlush   SystemEventType = iota // KV did not implement
	ScopeCreate       SystemEventType = iota
	ScopeDrop         SystemEventType = iota
	CollectionChanged SystemEventType = iota
)

type ScopeCreateEvent interface {
	GetSystemEventName() (string, error)
	GetScopeId() (uint32, error)
	GetManifestId() (uint64, error)
}

type CollectionCreateEvent interface {
	GetSystemEventName() (string, error)
	GetScopeId() (uint32, error)
	GetCollectionId() (uint32, error)
	GetManifestId() (uint64, error)
	GetMaxTTL() (uint32, error)
}

type CollectionDropEvent interface {
	GetScopeId() (uint32, error)
	GetCollectionId() (uint32, error)
	GetManifestId() (uint64, error)
}

type ScopeDropEvent interface {
	GetScopeId() (uint32, error)
	GetManifestId() (uint64, error)
}

type CollectionChangedEvent interface {
	GetCollectionId() (uint32, error)
	GetManifestId() (uint64, error)
	GetMaxTTL() (uint32, error)
}

var ErrorInvalidOp error = fmt.Errorf("Invalid Operation")
var ErrorInvalidVersion error = fmt.Errorf("Invalid version for parsing")
var ErrorValueTooShort error = fmt.Errorf("Value length is too short")
var ErrorNoMaxTTL error = fmt.Errorf("This event has no max TTL")

// UprEvent memcached events for UPR streams.
type UprEvent struct {
	Opcode          gomemcached.CommandCode // Type of event
	Status          gomemcached.Status      // Response status
	VBucket         uint16                  // VBucket this event applies to
	DataType        uint8                   // data type
	Opaque          uint16                  // 16 MSB of opaque
	VBuuid          uint64                  // This field is set by downstream
	Flags           uint32                  // Item flags
	Expiry          uint32                  // Item expiration time
	Key, Value      []byte                  // Item key/value
	OldValue        []byte                  // TODO: TBD: old document value
	Cas             uint64                  // CAS value of the item
	Seqno           uint64                  // sequence number of the mutation
	RevSeqno        uint64                  // rev sequence number : deletions
	LockTime        uint32                  // Lock time
	MetadataSize    uint16                  // Metadata size
	SnapstartSeq    uint64                  // start sequence number of this snapshot
	SnapendSeq      uint64                  // End sequence number of the snapshot
	SnapshotType    uint32                  // 0: disk 1: memory
	FailoverLog     *FailoverLog            // Failover log containing vvuid and sequnce number
	Error           error                   // Error value in case of a failure
	ExtMeta         []byte                  // Extended Metadata
	AckSize         uint32                  // The number of bytes that can be Acked to DCP
	SystemEvent     SystemEventType         // Only valid if IsSystemEvent() is true
	SysEventVersion uint8                   // Based on the version, the way Extra bytes is parsed is different
	ValueLen        int                     // Cache it to avoid len() calls for performance
	CollectionId    uint32                  // Valid if Collection is in use
	StreamId        *uint16                 // Nil if not in use
}

// FailoverLog containing vvuid and sequnce number
type FailoverLog [][2]uint64

// Containing a pair of vbno and the high seqno
type VBSeqnos [][2]uint64

func makeUprEvent(rq gomemcached.MCRequest, stream *UprStream, bytesReceivedFromDCP int) *UprEvent {
	event := &UprEvent{
		Opcode:       rq.Opcode,
		Value:        rq.Body,
		Cas:          rq.Cas,
		ExtMeta:      rq.ExtMeta,
		DataType:     rq.DataType,
		ValueLen:     len(rq.Body),
		SystemEvent:  InvalidSysEvent,
		CollectionId: math.MaxUint32,
	}

	if stream != nil {
		event.VBucket = stream.Vbucket
		event.VBuuid = stream.Vbuuid
		event.PopulateFieldsBasedOnStreamType(rq, stream.StreamType)
	} else {
		event.VBucket = vbOpaque(rq.Opaque)
	}

	// set AckSize for events that need to be acked to DCP,
	// i.e., events with CommandCodes that need to be buffered in DCP
	if _, ok := gomemcached.BufferedCommandCodeMap[rq.Opcode]; ok {
		event.AckSize = uint32(bytesReceivedFromDCP)
	}

	// 16 LSBits are used by client library to encode vbucket number.
	// 16 MSBits are left for application to multiplex on opaque value.
	event.Opaque = appOpaque(rq.Opaque)

	if len(rq.Extras) >= uprMutationExtraLen &&
		event.Opcode == gomemcached.UPR_MUTATION {

		event.Seqno = binary.BigEndian.Uint64(rq.Extras[:8])
		event.RevSeqno = binary.BigEndian.Uint64(rq.Extras[8:16])
		event.Flags = binary.BigEndian.Uint32(rq.Extras[16:20])
		event.Expiry = binary.BigEndian.Uint32(rq.Extras[20:24])
		event.LockTime = binary.BigEndian.Uint32(rq.Extras[24:28])
		event.MetadataSize = binary.BigEndian.Uint16(rq.Extras[28:30])

	} else if len(rq.Extras) >= uprDeletetionWithDeletionTimeExtraLen &&
		event.Opcode == gomemcached.UPR_DELETION {

		event.Seqno = binary.BigEndian.Uint64(rq.Extras[:8])
		event.RevSeqno = binary.BigEndian.Uint64(rq.Extras[8:16])
		event.Expiry = binary.BigEndian.Uint32(rq.Extras[16:20])

	} else if len(rq.Extras) >= uprDeletetionExtraLen &&
		event.Opcode == gomemcached.UPR_DELETION ||
		event.Opcode == gomemcached.UPR_EXPIRATION {

		event.Seqno = binary.BigEndian.Uint64(rq.Extras[:8])
		event.RevSeqno = binary.BigEndian.Uint64(rq.Extras[8:16])
		event.MetadataSize = binary.BigEndian.Uint16(rq.Extras[16:18])

	} else if len(rq.Extras) >= uprSnapshotExtraLen &&
		event.Opcode == gomemcached.UPR_SNAPSHOT {

		event.SnapstartSeq = binary.BigEndian.Uint64(rq.Extras[:8])
		event.SnapendSeq = binary.BigEndian.Uint64(rq.Extras[8:16])
		event.SnapshotType = binary.BigEndian.Uint32(rq.Extras[16:20])
	} else if event.IsSystemEvent() {
		event.PopulateEvent(rq.Extras)
	} else if event.IsSeqnoAdv() {
		event.PopulateSeqnoAdv(rq.Extras)
	} else if event.IsOsoSnapshot() {
		event.PopulateOso(rq.Extras)
	} else if event.IsStreamEnd() {
		event.PopulateStreamEndFlags(rq.Extras)
	}

	return event
}

func (event *UprEvent) PopulateFieldsBasedOnStreamType(rq gomemcached.MCRequest, streamType DcpStreamType) {
	switch streamType {
	case CollectionsStreamId:
		for _, extra := range rq.FramingExtras {
			streamId, streamIdErr := extra.GetStreamId()
			if streamIdErr == nil {
				event.StreamId = &streamId
			}
		}
		// After parsing streamID, still need to populate regular collectionID
		fallthrough
	case CollectionsNonStreamId:
		switch rq.Opcode {
		// Only these will have CID encoded within the key
		case gomemcached.UPR_MUTATION,
			gomemcached.UPR_DELETION,
			gomemcached.UPR_EXPIRATION:
			uleb128 := Uleb128(rq.Key)
			result, bytesShifted := uleb128.ToUint32(rq.Keylen)
			event.CollectionId = result
			event.Key = rq.Key[bytesShifted:]
		default:
			event.Key = rq.Key
		}
	case NonCollectionStream:
		// Let default behavior be legacy stream type
		fallthrough
	default:
		event.Key = rq.Key
	}
}

func (event *UprEvent) String() string {
	name := gomemcached.CommandNames[event.Opcode]
	if name == "" {
		name = fmt.Sprintf("#%d", event.Opcode)
	}
	return name
}

func (event *UprEvent) IsSnappyDataType() bool {
	return event.Opcode == gomemcached.UPR_MUTATION && (event.DataType&SnappyDataType > 0)
}

func (event *UprEvent) IsCollectionType() bool {
	return event.IsSystemEvent() || event.CollectionId <= math.MaxUint32
}

func (event *UprEvent) IsSystemEvent() bool {
	return event.Opcode == gomemcached.DCP_SYSTEM_EVENT
}

func (event *UprEvent) IsSeqnoAdv() bool {
	return event.Opcode == gomemcached.DCP_SEQNO_ADV
}

func (event *UprEvent) IsOsoSnapshot() bool {
	return event.Opcode == gomemcached.DCP_OSO_SNAPSHOT
}

func (event *UprEvent) IsStreamEnd() bool {
	return event.Opcode == gomemcached.UPR_STREAMEND
}

func (event *UprEvent) PopulateEvent(extras []byte) {
	if len(extras) < dcpSystemEventExtraLen {
		// Wrong length, don't parse
		return
	}

	event.Seqno = binary.BigEndian.Uint64(extras[:8])
	event.SystemEvent = SystemEventType(binary.BigEndian.Uint32(extras[8:12]))
	event.SysEventVersion = extras[12]
}

func (event *UprEvent) PopulateSeqnoAdv(extras []byte) {
	if len(extras) < dcpSeqnoAdvExtraLen {
		// Wrong length, don't parse
		return
	}

	event.Seqno = binary.BigEndian.Uint64(extras[:8])
}

func (event *UprEvent) PopulateOso(extras []byte) {
	if len(extras) < dcpOsoExtraLen {
		// Wrong length, don't parse
		return
	}
	event.Flags = binary.BigEndian.Uint32(extras[:4])
}

func (event *UprEvent) PopulateStreamEndFlags(extras []byte) {
	if len(extras) < dcpStreamEndExtraLen {
		// Wrong length, don't parse
		return
	}
	event.Flags = binary.BigEndian.Uint32(extras[:4])
}

func (event *UprEvent) GetSystemEventName() (string, error) {
	switch event.SystemEvent {
	case CollectionCreate:
		fallthrough
	case ScopeCreate:
		return string(event.Key), nil
	default:
		return "", ErrorInvalidOp
	}
}

func (event *UprEvent) GetManifestId() (uint64, error) {
	// non-flatbuffer-serialised data
	if event.SysEventVersion < 2 {
		switch event.SystemEvent {
		case ScopeDrop:
			fallthrough
		case ScopeCreate:
			fallthrough
		case CollectionDrop:
			if event.SysEventVersion > 0 { // "Version 0 only" check for this and the above event types
				return 0, ErrorInvalidVersion
			}
			fallthrough
		case CollectionCreate:
			if event.SysEventVersion > 1 { // CollectionCreate supports version 0 & 1
				return 0, ErrorInvalidVersion
			}
			if event.ValueLen < 8 {
				return 0, ErrorValueTooShort
			}
			return binary.BigEndian.Uint64(event.Value[0:8]), nil
		default:
			return 0, ErrorInvalidOp
		}
	}

	// 'version 2' system events are all flatbuffer serialised
	switch event.SystemEvent {
	case ScopeDrop:
		fb := systemevents.GetRootAsDroppedScope(event.Value, 0)
		return fb.Uid(), nil
	case ScopeCreate:
		fb := systemevents.GetRootAsScope(event.Value, 0)
		return fb.Uid(), nil
	case CollectionDrop:
		fb := systemevents.GetRootAsDroppedCollection(event.Value, 0)
		return fb.Uid(), nil
	case CollectionCreate:
		fallthrough
	case CollectionChanged:
		fb := systemevents.GetRootAsCollection(event.Value, 0)
		return fb.Uid(), nil
	default:
		return 0, ErrorInvalidOp
	}
}

func (event *UprEvent) GetCollectionId() (uint32, error) {
	// non-flatbuffer-serialised data
	if event.SysEventVersion < 2 {
		switch event.SystemEvent {
		case CollectionDrop:
			if event.SysEventVersion > 0 {
				return 0, ErrorInvalidVersion
			}
			fallthrough
		case CollectionCreate:
			if event.SysEventVersion > 1 {
				return 0, ErrorInvalidVersion
			}
			if event.ValueLen < 16 {
				return 0, ErrorValueTooShort
			}
			return binary.BigEndian.Uint32(event.Value[12:16]), nil
		default:
			return 0, ErrorInvalidOp
		}
	}

	// 'version 2' system events are all flatbuffer serialised
	switch event.SystemEvent {
	case CollectionDrop:
		fb := systemevents.GetRootAsDroppedCollection(event.Value, 0)
		return fb.CollectionId(), nil
	case CollectionCreate:
		fallthrough
	case CollectionChanged:
		fb := systemevents.GetRootAsCollection(event.Value, 0)
		return fb.CollectionId(), nil
	default:
		return 0, ErrorInvalidOp
	}
}

func (event *UprEvent) GetScopeId() (uint32, error) {
	// non-flatbuffer-serialised data
	if event.SysEventVersion < 2 {
		switch event.SystemEvent {
		case ScopeCreate:
			fallthrough
		case ScopeDrop:
			fallthrough
		case CollectionDrop:
			if event.SysEventVersion > 0 { // "Version 0 only" check for this and the above event types
				return 0, ErrorInvalidVersion
			}
			fallthrough
		case CollectionCreate:
			if event.SysEventVersion > 1 { // CollectionCreate supports version 0 & 1
				return 0, ErrorInvalidVersion
			}
			if event.ValueLen < 12 {
				return 0, ErrorValueTooShort
			}
			return binary.BigEndian.Uint32(event.Value[8:12]), nil
		default:
			return 0, ErrorInvalidOp
		}
	}

	// 'version 2' system events are all flatbuffer serialised
	switch event.SystemEvent {
	case ScopeDrop:
		fb := systemevents.GetRootAsDroppedScope(event.Value, 0)
		return fb.ScopeId(), nil
	case ScopeCreate:
		fb := systemevents.GetRootAsScope(event.Value, 0)
		return fb.ScopeId(), nil
	case CollectionDrop:
		fb := systemevents.GetRootAsDroppedCollection(event.Value, 0)
		return fb.ScopeId(), nil
	case CollectionCreate:
		fallthrough
	case CollectionChanged:
		fb := systemevents.GetRootAsCollection(event.Value, 0)
		return fb.ScopeId(), nil
	default:
		return 0, ErrorInvalidOp
	}
}

func (event *UprEvent) GetMaxTTL() (uint32, error) {
	// non-flatbuffer-serialised data
	if event.SysEventVersion < 2 {
		switch event.SystemEvent {
		case CollectionCreate:
			if event.SysEventVersion < 1 {
				return 0, ErrorNoMaxTTL
			}
			if event.ValueLen < 20 {
				return 0, ErrorValueTooShort
			}
			return binary.BigEndian.Uint32(event.Value[16:20]), nil
		default:
			return 0, ErrorInvalidOp
		}
	}

	// 'version 2' system events are all flatbuffer serialised
	switch event.SystemEvent {
	case CollectionCreate:
		fallthrough
	case CollectionChanged:
		fb := systemevents.GetRootAsCollection(event.Value, 0)
		return fb.MaxTtl(), nil
	default:
		return 0, ErrorInvalidOp
	}
}

// Only if error is nil:
// Returns true if event states oso begins
// Return false if event states oso ends
func (event *UprEvent) GetOsoBegin() (bool, error) {
	if !event.IsOsoSnapshot() {
		return false, ErrorInvalidOp
	}

	if event.Flags == 1 {
		return true, nil
	} else if event.Flags == 2 {
		return false, nil
	} else {
		return false, ErrorInvalidOp
	}
}

type Uleb128 []byte

func (u Uleb128) ToUint32(cachedLen int) (result uint32, bytesShifted int) {
	var shift uint = 0

	for curByte := 0; curByte < cachedLen; curByte++ {
		oneByte := u[curByte]
		last7Bits := 0x7f & oneByte
		result |= uint32(last7Bits) << shift
		bytesShifted++
		if oneByte&0x80 == 0 {
			break
		}
		shift += 7
	}

	return
}
