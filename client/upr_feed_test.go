package memcached

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/couchbase/gomemcached"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func setupBoilerPlate() (*vbStreamNegotiator, *UprFeed) {
	negotiator := &vbStreamNegotiator{}
	negotiator.initialize()

	testFeed := &UprFeed{
		vbstreams:  make(map[uint16]*UprStream),
		negotiator: *negotiator,
	}

	return negotiator, testFeed
}

func TestNegotiator(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNegotiator =================")
	var vbno uint16 = 1
	var opaque uint16 = 2
	opaqueComposed := composeOpaque(vbno, opaque)
	var headerBuf [gomemcached.HDR_LEN]byte

	negotiator, testFeed := setupBoilerPlate()

	_, err := negotiator.getStreamFromMap(1, 2)
	assert.NotNil(err)

	negotiator.registerRequest(vbno, opaque, 3, 4, 5)
	_, err = negotiator.getStreamFromMap(vbno, opaque)
	assert.Nil(err)

	err = testFeed.validateCloseStream(vbno)
	assert.Nil(err)

	request := &gomemcached.MCRequest{Opcode: gomemcached.UPR_STREAMREQ,
		VBucket: vbno,
		Opaque:  opaqueComposed,
	}
	response := &gomemcached.MCResponse{Opcode: gomemcached.UPR_STREAMREQ,
		Opaque: opaqueComposed,
	}

	event, err := negotiator.handleStreamRequest(testFeed, headerBuf, request, 0, response)
	assert.Nil(err)
	assert.NotNil(event)

	fmt.Println("============== Test case end: TestNegotiator =================")
}

func TestNegotiatorMultiSession(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNegotiatorMultiSession =================")
	var vbno uint16 = 1
	var opaque uint16 = 2
	opaqueComposed := composeOpaque(vbno, opaque)
	var headerBuf [gomemcached.HDR_LEN]byte

	negotiator, testFeed := setupBoilerPlate()

	negotiator.registerRequest(vbno, opaque, 3, 4, 5)
	_, err := negotiator.getStreamFromMap(vbno, opaque)
	assert.Nil(err)

	negotiator.registerRequest(vbno, opaque+1, 3, 4, 5)
	_, err = negotiator.getStreamFromMap(vbno, opaque+1)
	assert.Nil(err)

	request := &gomemcached.MCRequest{Opcode: gomemcached.UPR_STREAMREQ,
		VBucket: vbno,
		Opaque:  opaqueComposed,
	}

	// Assume a response from DCP
	rollbackNumberBuffer := new(bytes.Buffer)
	err = binary.Write(rollbackNumberBuffer, binary.BigEndian, uint64(0))
	assert.Nil(err)

	response := &gomemcached.MCResponse{Opcode: gomemcached.UPR_STREAMREQ,
		Opaque: opaqueComposed,
		Status: gomemcached.ROLLBACK,
		Body:   rollbackNumberBuffer.Bytes(),
	}

	event, err := negotiator.handleStreamRequest(testFeed, headerBuf, request, 0, response)
	assert.Nil(err)
	assert.NotNil(event)

	// After a success, the map should be empty for this one
	_, err = negotiator.getStreamFromMap(vbno, opaque)
	assert.NotNil(err)

	// The second one should still be there
	_, err = negotiator.getStreamFromMap(vbno, opaque+1)
	assert.Nil(err)

	response.Opaque = composeOpaque(vbno, opaque+1)
	event, err = negotiator.handleStreamRequest(testFeed, headerBuf, request, 0, response)
	assert.Nil(err)
	assert.NotNil(event)

	_, err = negotiator.getStreamFromMap(vbno, opaque+1)
	assert.NotNil(err)

	fmt.Println("============== Test case end: TestNegotiatorMultiSession =================")
}

func retrieveMcRequest(fileName string) *gomemcached.MCRequest {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		panic(err.Error())
	}

	request := &gomemcached.MCRequest{}
	err = json.Unmarshal(data, request)
	if err != nil {
		panic(err.Error())
	}

	return request
}

func TestCreateScopeEvent(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case TestCreateScopeEvent =================")

	scopeCreationFile := "./unitTestData/scopeCreation.json"
	mcReq := retrieveMcRequest(scopeCreationFile)

	assert.NotNil(mcReq)

	assert.Equal(uint16(511), vbOpaque(mcReq.Opaque))

	tempStream := &UprStream{Vbucket: mcReq.VBucket}
	tempBytes := 1024

	event := makeUprEvent(*mcReq, tempStream, tempBytes)
	assert.Equal(gomemcached.DCP_SYSTEM_EVENT, event.Opcode)
	assert.True(event.IsCollectionType())
	// The key should be the name of the scope being created
	assert.Equal([]byte("S1"), event.Key)
	assert.Equal(event.SystemEvent, ScopeCreate)

	creationEvent := ScopeCreateEvent(event)
	checkScopeName, err := creationEvent.GetSystemEventName()
	assert.Nil(err)
	assert.Equal("S1", checkScopeName)

	var manifestUid uint64 = binary.BigEndian.Uint64(mcReq.Body[0:8])
	checkManifest, err := creationEvent.GetManifestId()
	assert.Nil(err)
	assert.Equal(manifestUid, checkManifest)

	var scopeId uint32 = 8
	checkScopeId, err := creationEvent.GetScopeId()
	assert.Nil(err)
	assert.Equal(scopeId, checkScopeId)

	// Extras - first uint64 is "by_seqno" - the sequence number of this event
	var bySeqno uint64 = binary.BigEndian.Uint64(mcReq.Extras[:8])
	assert.Equal(bySeqno, event.Seqno)

	fmt.Println("============== Test case TestCreateScopeEvent =================")
}

func TestCreateCollectionEvent(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case TestCreateCollectionEvent =================")

	creationFile := "./unitTestData/collectionCreation.json"
	mcReq := retrieveMcRequest(creationFile)

	assert.NotNil(mcReq)

	assert.Equal(uint16(511), vbOpaque(mcReq.Opaque))

	tempStream := &UprStream{Vbucket: mcReq.VBucket}
	tempBytes := 1024

	event := makeUprEvent(*mcReq, tempStream, tempBytes)
	assert.Equal(gomemcached.DCP_SYSTEM_EVENT, event.Opcode)
	assert.True(event.IsCollectionType())
	// The key should be the name of the scope being created
	assert.Equal([]byte("C1"), event.Key)
	assert.Equal(event.SystemEvent, CollectionCreate)

	creationEvent := CollectionCreateEvent(event)
	checkName, err := creationEvent.GetSystemEventName()
	assert.Nil(err)
	assert.Equal("C1", checkName)

	var manifestUid uint64 = binary.BigEndian.Uint64(mcReq.Body[0:8])
	checkManifest, err := creationEvent.GetManifestId()
	assert.Nil(err)
	assert.Equal(manifestUid, checkManifest)

	var scopeId uint32 = 8
	checkId, err := creationEvent.GetScopeId()
	assert.Nil(err)
	assert.Equal(scopeId, checkId)

	var collectionId uint32 = 9
	checkId, err = creationEvent.GetCollectionId()
	assert.Nil(err)
	assert.Equal(collectionId, checkId)

	// Extras - first uint64 is "by_seqno" - the sequence number of this event
	var bySeqno uint64 = binary.BigEndian.Uint64(mcReq.Extras[:8])
	assert.Equal(bySeqno, event.Seqno)

	fmt.Println("============== Test case TestCreateCollectionEvent =================")
}

func TestDeleteCollectionEvent(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case TestDeleteCollectionEvent =================")

	deletionFile := "./unitTestData/collectionDel.json"
	mcReq := retrieveMcRequest(deletionFile)

	assert.NotNil(mcReq)

	// Collection drop does not send key, so vbno is not going to be the same
	tempStream := &UprStream{Vbucket: mcReq.VBucket}
	tempBytes := 1024

	event := makeUprEvent(*mcReq, tempStream, tempBytes)
	assert.Equal(gomemcached.DCP_SYSTEM_EVENT, event.Opcode)
	assert.True(event.IsCollectionType())
	assert.Equal([]byte(""), event.Key)
	assert.Equal(event.SystemEvent, CollectionDrop)

	delEvent := CollectionDropEvent(event)

	var manifestUid uint64 = binary.BigEndian.Uint64(mcReq.Body[0:8])
	checkManifest, err := delEvent.GetManifestId()
	assert.Nil(err)
	assert.Equal(manifestUid, checkManifest)

	var scopeId uint32 = 8
	checkId, err := delEvent.GetScopeId()
	assert.Nil(err)
	assert.Equal(scopeId, checkId)

	// Unfortunately the same but it's ok
	var collectionId uint32 = 9
	checkId, err = delEvent.GetCollectionId()
	assert.Nil(err)
	assert.Equal(collectionId, checkId)

	// Extras - first uint64 is "by_seqno" - the sequence number of this event
	var bySeqno uint64 = binary.BigEndian.Uint64(mcReq.Extras[:8])
	assert.Equal(bySeqno, event.Seqno)

	fmt.Println("============== Test case TestDeleteCollectionEvent =================")
}

func TestDeleteScopeEvent(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case TestDeleteScopeEvent =================")

	deletionFile := "./unitTestData/scopeDel.json"
	mcReq := retrieveMcRequest(deletionFile)
	assert.NotNil(mcReq)

	// Collection drop does not send key, so vbno is not going to be the same
	tempStream := &UprStream{Vbucket: mcReq.VBucket}
	tempBytes := 1024

	event := makeUprEvent(*mcReq, tempStream, tempBytes)
	assert.Equal(gomemcached.DCP_SYSTEM_EVENT, event.Opcode)
	assert.True(event.IsCollectionType())
	assert.Equal([]byte(""), event.Key)
	assert.Equal(event.SystemEvent, ScopeDrop)

	delEvent := ScopeDropEvent(event)

	var manifestUid uint64 = binary.BigEndian.Uint64(mcReq.Body[0:8])
	checkManifest, err := delEvent.GetManifestId()
	assert.Nil(err)
	assert.Equal(manifestUid, checkManifest)

	var scopeId uint32 = 8
	checkId, err := delEvent.GetScopeId()
	assert.Nil(err)
	assert.Equal(scopeId, checkId)

	// Extras - first uint64 is "by_seqno" - the sequence number of this event
	var bySeqno uint64 = binary.BigEndian.Uint64(mcReq.Extras[:8])
	assert.Equal(bySeqno, event.Seqno)

	fmt.Println("============== Test case TestDeleteScopeEvent =================")
}

func TestLegacyNoCollectionMutation(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case TestLegacyNoCollectionMutation =================")

	mutationFile := "./unitTestData/origNoCollectionMut.json"
	mcReq := retrieveMcRequest(mutationFile)
	assert.NotNil(mcReq)

	tempStream := &UprStream{Vbucket: mcReq.VBucket}
	tempBytes := 1024
	event := makeUprEvent(*mcReq, tempStream, tempBytes)
	assert.False(event.IsSystemEvent())

	assert.Equal("d1", string(event.Key))

	fmt.Println("============== Test case TestLegacyNoCollectionMutation =================")
}

func TestDefaultScopeMutation(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case TestDefaultScopeMutation =================")

	mutationFile := "./unitTestData/defaultScopeDefaultColMutation.json"
	mcReq := retrieveMcRequest(mutationFile)
	assert.NotNil(mcReq)

	// The StreamType activate uleb128 parsing of key
	tempStream := &UprStream{Vbucket: mcReq.VBucket, StreamType: CollectionsNonStreamId}
	tempBytes := 1024
	event := makeUprEvent(*mcReq, tempStream, tempBytes)
	assert.False(event.IsSystemEvent())

	assert.Equal("d1", string(event.Key))
	assert.Equal(uint64(0), event.CollectionId)

	fmt.Println("============== Test case TestDefaultScopeMutation =================")
}

func TestNonDefaultScopeMutation(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case TestNonDefaultScopeMutation =================")

	mutationFile := "./unitTestData/scope_and_collection_mutation.json"
	mcReq := retrieveMcRequest(mutationFile)
	assert.NotNil(mcReq)

	// The StreamType activate uleb128 parsing of key
	tempStream := &UprStream{Vbucket: mcReq.VBucket, StreamType: CollectionsNonStreamId}
	tempBytes := 1024
	event := makeUprEvent(*mcReq, tempStream, tempBytes)
	assert.False(event.IsSystemEvent())

	// Manifest version 4
	// ScopeName S1 (UID 8)
	// ScopeName S1 CollectionName C1 (UID 9)

	assert.Equal("d1", string(event.Key))
	assert.Equal(`{"key":"a sentence"}`, string(event.Value))
	assert.True(event.IsCollectionType())
	assert.Equal(uint64(9), event.CollectionId)

	fmt.Println("============== Test case TestNonDefaultScopeMutation =================")
}
