package redis

import (
	"fmt"
	"io"

	"github.com/segmentio/objconv"
	"github.com/segmentio/objconv/resp"
)

// Message represents a message exchanged when redis is used in PUB/SUB mode.
type Message struct {
	// Type is the type of the redis message with can be one of "subscribe",
	// "unsubscribe", "message", "psubscribe", "punsubscribe" or "pmessage".
	//
	// See http://redis.io/topics/pubsub for more information.
	Type string

	// Channel is the channel that this message is published ot.
	Channel []byte

	// Payload is the actual content of the message, this should be empty if the
	// message type is not "message" or "pmessage".
	Payload []byte

	// Subscriptions is the number of channels that a client is subscribed to,
	// it should only be set for "subscribe", "unsubscribe", "psubscribe" and
	// "punsubscribe" messages.
	Subscriptions int
}

// NewMessage returns a pointer to a new Message initialized with mtype, channel
// and payload.
//
// The type of the returned message is "message".
func NewMessage(channel string, payload string) *Message {
	return &Message{
		Type:    "message",
		Channel: []byte(channel),
		Payload: []byte(payload),
	}
}

// Write outputs the message to w, returning an error if something went wrong.
func (m *Message) Write(w io.Writer) (err error) {
	v := []interface{}{[]byte(m.Type), m.Channel, nil}

	switch m.Type {
	case "subscribe", "unsubscribe", "psubscribe", "punsubscribe":
		v[2] = m.Subscriptions
	default:
		v[2] = m.Payload
	}

	return objconv.NewEncoder(objconv.EncoderConfig{
		Output:  w,
		Emitter: &resp.Emitter{},
	}).Encode(v)
}

// ReadMessage reads and returns a redis message for r, or returns an error if
// something went wrong.
func ReadMessage(r io.Reader) (msg *Message, err error) {
	dec := objconv.NewStreamDecoder(objconv.DecoderConfig{
		Input:  r,
		Parser: &resp.Parser{},
	})
	n := dec.Len()

	if err = dec.Error(); err != nil {
		return
	}

	if n != 3 {
		err = fmt.Errorf("redis.ReadMessage: invalid array length encountered, expected 3 but got %d", n)
		return
	}

	var m = &Message{}
	var v interface{}

	if err = dec.Decode(&m.Type); err != nil {
		return
	}

	if err = dec.Decode(&m.Channel); err != nil {
		return
	}

	if err = dec.Decode(&v); err != nil {
		return
	}

	switch m.Type {
	case "subscribe", "unsubscribe", "psubscribe", "punsubscribe":
		switch i := v.(type) {
		case int64:
			m.Subscriptions = int(i)
		default:
			err = fmt.Errorf("redis.ReadMessage: expected subscribed channel count but got '%v'", v)
			return
		}
	default:
		switch s := v.(type) {
		case string:
			m.Payload = []byte(s)
		case []byte:
			m.Payload = s
		default:
			err = fmt.Errorf("redis.ReadMessage: expected message payload but got '%s'", v)
			return
		}
	}

	msg = m
	return
}
