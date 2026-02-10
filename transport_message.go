package theatre

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"math"
	"sync"
)

// Transport wire-protocol tags.
//
// Frame format: [4-byte big-endian payload length][1-byte tag][binary-encoded message]
// Payload length covers the tag byte plus the encoded bytes.
const (
	TagActorForward      byte = 1
	TagActorForwardReply byte = 2
	TagNotHere           byte = 3
	TagHostFrozen        byte = 4
	TagPing              byte = 5
	TagPong              byte = 6
	TagBatch             byte = 0x10
)

// Body type tags for the custom wire encoding of interface{} fields.
// Common types (string, int, etc.) are encoded directly to avoid reflection.
// Unknown types fall back to gob encoding.
const (
	bodyNil     byte = 0
	bodyString  byte = 1
	bodyInt     byte = 2
	bodyInt64   byte = 3
	bodyFloat64 byte = 4
	bodyBool    byte = 5
	bodyBytes   byte = 6
	bodyGob     byte = 7
)

// ActorForward requests delivery of a message to an actor on a remote host.
type ActorForward struct {
	ActorType    string
	ActorID      string
	Body         interface{}
	ReplyID      int64
	SenderHostID string
}

// ActorForwardReply carries the response to a forwarded request.
type ActorForwardReply struct {
	ReplyID int64
	Body    interface{}
	Error   string
}

// NotHere tells the sender that the specified actor is not activated on this host.
type NotHere struct {
	ActorType string
	ActorID   string
	HostID    string
	Epoch     int64
}

// HostFrozen tells the sender that this host has lost its lease and is frozen.
// The ActorType/ActorID fields identify which actor forward triggered the response.
type HostFrozen struct {
	ActorType string
	ActorID   string
	ReplyID   int64
	HostID    string
	Epoch     int64
}

// TransportPing is a liveness probe.
type TransportPing struct{}

// TransportPong is the reply to a TransportPing.
type TransportPong struct{}

// TransportEnvelope is a tagged transport-layer message.
type TransportEnvelope struct {
	Tag     byte
	Payload interface{} // one of *ActorForward, *ActorForwardReply, *NotHere, *HostFrozen, *TransportPing, *TransportPong
}

// Pools for the two highest-volume transport message types.
// These structs are allocated per-message on both the encode (routing.go)
// and decode (decodePayload) paths. Pooling them eliminates ~6 GB of
// allocations per 15 s at 3.5 M msg/s.
var actorForwardPool = sync.Pool{
	New: func() any { return &ActorForward{} },
}

var actorForwardReplyPool = sync.Pool{
	New: func() any { return &ActorForwardReply{} },
}

// recyclePayload zeros a pooled struct and returns it to its pool.
// Safe to call on any TransportEnvelope — non-pooled types are ignored.
func recyclePayload(env TransportEnvelope) {
	switch env.Tag {
	case TagActorForward:
		if msg, ok := env.Payload.(*ActorForward); ok {
			*msg = ActorForward{}
			actorForwardPool.Put(msg)
		}
	case TagActorForwardReply:
		if msg, ok := env.Payload.(*ActorForwardReply); ok {
			*msg = ActorForwardReply{}
			actorForwardReplyPool.Put(msg)
		}
	}
}

// recycleEnvelopes recycles pooled payloads and clears references in a
// batch slice so the GC doesn't keep returned structs alive via the array.
func recycleEnvelopes(envs []TransportEnvelope) {
	for i := range envs {
		recyclePayload(envs[i])
		envs[i] = TransportEnvelope{}
	}
}

// Envelope creates a TransportEnvelope with the tag inferred from the payload type.
// Returns an error if the payload is not a recognized transport message type.
// This function never panics — callers on network paths must handle the error
// and close the connection cleanly rather than crashing the host.
func Envelope(payload interface{}) (TransportEnvelope, error) {
	var tag byte
	switch payload.(type) {
	case ActorForward, *ActorForward:
		tag = TagActorForward
	case ActorForwardReply, *ActorForwardReply:
		tag = TagActorForwardReply
	case NotHere, *NotHere:
		tag = TagNotHere
	case HostFrozen, *HostFrozen:
		tag = TagHostFrozen
	case TransportPing, *TransportPing:
		tag = TagPing
	case TransportPong, *TransportPong:
		tag = TagPong
	default:
		return TransportEnvelope{}, fmt.Errorf("theatre: unknown transport message type %T", payload)
	}
	return TransportEnvelope{Tag: tag, Payload: payload}, nil
}

func init() {
	// Register basic types for the gob fallback path used when Body
	// contains types not handled by the native binary codec.
	gob.Register("")
	gob.Register(0)
	gob.Register(int64(0))
	gob.Register(float64(0))
	gob.Register(false)
	gob.Register([]byte(nil))
	gob.Register(map[string]interface{}{})
}

// RegisterGobType registers a user-defined type so it can be transmitted
// as a Body value via the gob fallback path. Must be called before sending
// messages containing this type.
func RegisterGobType(value interface{}) {
	gob.Register(value)
}

// --- binary codec: encode ---

// encodePayload writes the binary-encoded payload fields into buf.
func encodePayload(buf *bytes.Buffer, env TransportEnvelope) error {
	switch env.Tag {
	case TagActorForward:
		var msg *ActorForward
		switch v := env.Payload.(type) {
		case *ActorForward:
			msg = v
		case ActorForward:
			msg = &v
		default:
			return fmt.Errorf("expected ActorForward, got %T", env.Payload)
		}
		putStr(buf, msg.ActorType)
		putStr(buf, msg.ActorID)
		putI64(buf, msg.ReplyID)
		putStr(buf, msg.SenderHostID)
		return putBody(buf, msg.Body)

	case TagActorForwardReply:
		var msg *ActorForwardReply
		switch v := env.Payload.(type) {
		case *ActorForwardReply:
			msg = v
		case ActorForwardReply:
			msg = &v
		default:
			return fmt.Errorf("expected ActorForwardReply, got %T", env.Payload)
		}
		putI64(buf, msg.ReplyID)
		putStr(buf, msg.Error)
		return putBody(buf, msg.Body)

	case TagNotHere:
		var msg *NotHere
		switch v := env.Payload.(type) {
		case *NotHere:
			msg = v
		case NotHere:
			msg = &v
		default:
			return fmt.Errorf("expected NotHere, got %T", env.Payload)
		}
		putStr(buf, msg.ActorType)
		putStr(buf, msg.ActorID)
		putStr(buf, msg.HostID)
		putI64(buf, msg.Epoch)
		return nil

	case TagHostFrozen:
		var msg *HostFrozen
		switch v := env.Payload.(type) {
		case *HostFrozen:
			msg = v
		case HostFrozen:
			msg = &v
		default:
			return fmt.Errorf("expected HostFrozen, got %T", env.Payload)
		}
		putStr(buf, msg.ActorType)
		putStr(buf, msg.ActorID)
		putI64(buf, msg.ReplyID)
		putStr(buf, msg.HostID)
		putI64(buf, msg.Epoch)
		return nil

	case TagPing, TagPong:
		return nil

	default:
		return fmt.Errorf("unknown tag %d", env.Tag)
	}
}

func putStr(buf *bytes.Buffer, s string) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(len(s)))
	buf.Write(tmp[:])
	buf.WriteString(s)
}

func putI64(buf *bytes.Buffer, v int64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	buf.Write(tmp[:])
}

func putBody(buf *bytes.Buffer, body interface{}) error {
	switch v := body.(type) {
	case nil:
		buf.WriteByte(bodyNil)
	case string:
		buf.WriteByte(bodyString)
		var tmp [4]byte
		binary.BigEndian.PutUint32(tmp[:], uint32(len(v)))
		buf.Write(tmp[:])
		buf.WriteString(v)
	case int:
		buf.WriteByte(bodyInt)
		putI64(buf, int64(v))
	case int64:
		buf.WriteByte(bodyInt64)
		putI64(buf, v)
	case float64:
		buf.WriteByte(bodyFloat64)
		var tmp [8]byte
		binary.BigEndian.PutUint64(tmp[:], math.Float64bits(v))
		buf.Write(tmp[:])
	case bool:
		buf.WriteByte(bodyBool)
		if v {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	case []byte:
		buf.WriteByte(bodyBytes)
		var tmp [4]byte
		binary.BigEndian.PutUint32(tmp[:], uint32(len(v)))
		buf.Write(tmp[:])
		buf.Write(v)
	default:
		// Gob fallback for user-defined types.
		buf.WriteByte(bodyGob)
		var gobBuf bytes.Buffer
		if err := gob.NewEncoder(&gobBuf).Encode(&body); err != nil {
			return fmt.Errorf("body gob encode: %w", err)
		}
		var tmp [4]byte
		binary.BigEndian.PutUint32(tmp[:], uint32(gobBuf.Len()))
		buf.Write(tmp[:])
		buf.Write(gobBuf.Bytes())
	}
	return nil
}

// --- batch codec ---

// encodeBatchPayload writes N sub-messages into buf using the batch wire format:
//
//	[2-byte count]
//	  [1-byte sub-tag][4-byte sub-payload-len][sub-payload-bytes]  × count
func encodeBatchPayload(buf *bytes.Buffer, envs []TransportEnvelope) error {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(len(envs)))
	buf.Write(tmp[:])
	for _, env := range envs {
		buf.WriteByte(env.Tag)
		// Reserve 4 bytes for sub-payload length.
		lenPos := buf.Len()
		var placeholder [4]byte
		buf.Write(placeholder[:])
		startPos := buf.Len()
		if err := encodePayload(buf, env); err != nil {
			return err
		}
		subLen := buf.Len() - startPos
		binary.BigEndian.PutUint32(buf.Bytes()[lenPos:], uint32(subLen))
	}
	return nil
}

// decodeBatchPayload reads a batch of sub-messages from data.
func decodeBatchPayload(data []byte) ([]TransportEnvelope, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("batch: short data for count")
	}
	count := int(binary.BigEndian.Uint16(data[:2]))
	off := 2

	envs := make([]TransportEnvelope, count)
	for i := 0; i < count; i++ {
		if off >= len(data) {
			return nil, fmt.Errorf("batch: short data for sub-tag at index %d", i)
		}
		tag := data[off]
		off++
		if off+4 > len(data) {
			return nil, fmt.Errorf("batch: short data for sub-length at index %d", i)
		}
		subLen := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if off+subLen > len(data) {
			return nil, fmt.Errorf("batch: short data for sub-payload at index %d", i)
		}
		payload, err := decodePayload(tag, data[off:off+subLen])
		if err != nil {
			return nil, fmt.Errorf("batch sub %d: %w", i, err)
		}
		envs[i] = TransportEnvelope{Tag: tag, Payload: payload}
		off += subLen
	}
	return envs, nil
}

// decodeBatchInto decodes a batch of sub-messages from data into the
// caller-provided buffer, avoiding the per-batch slice allocation.
// Returns the number of messages decoded.
func decodeBatchInto(data []byte, buf []TransportEnvelope) (int, error) {
	if len(data) < 2 {
		return 0, fmt.Errorf("batch: short data for count")
	}
	count := int(binary.BigEndian.Uint16(data[:2]))
	if count > len(buf) {
		return 0, fmt.Errorf("batch: count %d exceeds buffer %d", count, len(buf))
	}
	off := 2

	for i := 0; i < count; i++ {
		if off >= len(data) {
			return 0, fmt.Errorf("batch: short data for sub-tag at index %d", i)
		}
		tag := data[off]
		off++
		if off+4 > len(data) {
			return 0, fmt.Errorf("batch: short data for sub-length at index %d", i)
		}
		subLen := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if off+subLen > len(data) {
			return 0, fmt.Errorf("batch: short data for sub-payload at index %d", i)
		}
		payload, err := decodePayload(tag, data[off:off+subLen])
		if err != nil {
			return 0, fmt.Errorf("batch sub %d: %w", i, err)
		}
		buf[i] = TransportEnvelope{Tag: tag, Payload: payload}
		off += subLen
	}
	return count, nil
}

// --- binary codec: decode ---

// decodePayload reads a payload from data based on the tag.
func decodePayload(tag byte, data []byte) (interface{}, error) {
	switch tag {
	case TagActorForward:
		msg := actorForwardPool.Get().(*ActorForward)
		off := 0
		var err error
		if msg.ActorType, off, err = getStr(data, off); err != nil {
			actorForwardPool.Put(msg)
			return nil, err
		}
		if msg.ActorID, off, err = getStr(data, off); err != nil {
			actorForwardPool.Put(msg)
			return nil, err
		}
		if msg.ReplyID, off, err = getI64(data, off); err != nil {
			actorForwardPool.Put(msg)
			return nil, err
		}
		if msg.SenderHostID, off, err = getStr(data, off); err != nil {
			actorForwardPool.Put(msg)
			return nil, err
		}
		if msg.Body, _, err = getBody(data, off); err != nil {
			actorForwardPool.Put(msg)
			return nil, err
		}
		return msg, nil

	case TagActorForwardReply:
		msg := actorForwardReplyPool.Get().(*ActorForwardReply)
		off := 0
		var err error
		if msg.ReplyID, off, err = getI64(data, off); err != nil {
			actorForwardReplyPool.Put(msg)
			return nil, err
		}
		if msg.Error, off, err = getStr(data, off); err != nil {
			actorForwardReplyPool.Put(msg)
			return nil, err
		}
		if msg.Body, _, err = getBody(data, off); err != nil {
			actorForwardReplyPool.Put(msg)
			return nil, err
		}
		return msg, nil

	case TagNotHere:
		var msg NotHere
		off := 0
		var err error
		if msg.ActorType, off, err = getStr(data, off); err != nil {
			return nil, err
		}
		if msg.ActorID, off, err = getStr(data, off); err != nil {
			return nil, err
		}
		if msg.HostID, off, err = getStr(data, off); err != nil {
			return nil, err
		}
		if msg.Epoch, _, err = getI64(data, off); err != nil {
			return nil, err
		}
		return &msg, nil

	case TagHostFrozen:
		var msg HostFrozen
		off := 0
		var err error
		if msg.ActorType, off, err = getStr(data, off); err != nil {
			return nil, err
		}
		if msg.ActorID, off, err = getStr(data, off); err != nil {
			return nil, err
		}
		if msg.ReplyID, off, err = getI64(data, off); err != nil {
			return nil, err
		}
		if msg.HostID, off, err = getStr(data, off); err != nil {
			return nil, err
		}
		if msg.Epoch, _, err = getI64(data, off); err != nil {
			return nil, err
		}
		return &msg, nil

	case TagPing:
		return &TransportPing{}, nil
	case TagPong:
		return &TransportPong{}, nil
	case TagBatch:
		return decodeBatchPayload(data)
	default:
		return nil, fmt.Errorf("unknown tag %d", tag)
	}
}

func getStr(data []byte, off int) (string, int, error) {
	if off+2 > len(data) {
		return "", off, fmt.Errorf("short data for string length")
	}
	n := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	if off+n > len(data) {
		return "", off, fmt.Errorf("short data for string")
	}
	return string(data[off : off+n]), off + n, nil
}

func getI64(data []byte, off int) (int64, int, error) {
	if off+8 > len(data) {
		return 0, off, fmt.Errorf("short data for int64")
	}
	return int64(binary.BigEndian.Uint64(data[off:])), off + 8, nil
}

func getBody(data []byte, off int) (interface{}, int, error) {
	if off >= len(data) {
		return nil, off, fmt.Errorf("short data for body tag")
	}
	tag := data[off]
	off++
	switch tag {
	case bodyNil:
		return nil, off, nil
	case bodyString:
		if off+4 > len(data) {
			return nil, off, fmt.Errorf("short data for string body length")
		}
		n := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if off+n > len(data) {
			return nil, off, fmt.Errorf("short data for string body")
		}
		return string(data[off : off+n]), off + n, nil
	case bodyInt:
		v, newOff, err := getI64(data, off)
		return int(v), newOff, err
	case bodyInt64:
		return getI64(data, off)
	case bodyFloat64:
		if off+8 > len(data) {
			return nil, off, fmt.Errorf("short data for float64")
		}
		return math.Float64frombits(binary.BigEndian.Uint64(data[off:])), off + 8, nil
	case bodyBool:
		if off >= len(data) {
			return nil, off, fmt.Errorf("short data for bool")
		}
		return data[off] != 0, off + 1, nil
	case bodyBytes:
		if off+4 > len(data) {
			return nil, off, fmt.Errorf("short data for bytes length")
		}
		n := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if off+n > len(data) {
			return nil, off, fmt.Errorf("short data for bytes body")
		}
		b := make([]byte, n)
		copy(b, data[off:off+n])
		return b, off + n, nil
	case bodyGob:
		if off+4 > len(data) {
			return nil, off, fmt.Errorf("short data for gob length")
		}
		n := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if off+n > len(data) {
			return nil, off, fmt.Errorf("short data for gob body")
		}
		var body interface{}
		if err := gob.NewDecoder(bytes.NewReader(data[off : off+n])).Decode(&body); err != nil {
			return nil, off + n, fmt.Errorf("body gob decode: %w", err)
		}
		return body, off + n, nil
	default:
		return nil, off, fmt.Errorf("unknown body tag %d", tag)
	}
}
