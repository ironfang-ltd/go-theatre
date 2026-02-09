package theatre

import (
	"encoding/gob"
	"fmt"
)

// Transport wire-protocol tags.
//
// Frame format: [4-byte big-endian payload length][1-byte tag][gob-encoded message]
// Payload length covers the tag byte plus the gob bytes.
const (
	TagActorForward      byte = 1
	TagActorForwardReply byte = 2
	TagNotHere           byte = 3
	TagHostFrozen        byte = 4
	TagPing              byte = 5
	TagPong              byte = 6
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
	Error   string // gob cannot encode the error interface; transmitted as string
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
	// Register basic types that may appear in Body interface{} fields.
	// Without registration gob cannot encode/decode interface{} values.
	gob.Register("")
	gob.Register(0)
	gob.Register(int64(0))
	gob.Register(float64(0))
	gob.Register(false)
	gob.Register([]byte(nil))
	gob.Register(map[string]interface{}{})
}

// RegisterGobType registers a user-defined type for gob encoding/decoding.
// Must be called before sending messages containing this type as a Body value.
func RegisterGobType(value interface{}) {
	gob.Register(value)
}
