package xqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type AckFunc func() error

type NackFunc func() error

type Message struct {
	Ctx             context.Context
	ID              string
	UUID            string
	Payload         []byte
	Topic           string
	Timestamp       time.Time
	Metadata        map[string]interface{}
	RedeliveryCount int
	ackFn           AckFunc
	nackFn          NackFunc
}

func (m Message) String() string {
	return fmt.Sprintf("id[%v] uuid[%v] topic[%v] time[%v] redelivery[%v] data[%s]",
		m.ID, m.UUID, m.Topic, m.Timestamp.Format("2006-01-02 15:04:05.999"), m.RedeliveryCount, string(m.Payload))
}

func (m Message) Unmarshal(v interface{}) error {
	return json.Unmarshal(m.Payload, v)
}

func (m Message) Marshal(v interface{}) (err error) {
	m.Payload, err = json.Marshal(v)
	return
}

func (m Message) Ack() error {
	if m.ackFn == nil {
		return fmt.Errorf("ack function is nil")
	}
	return m.ackFn()
}

func (m Message) SetAckFunc(f AckFunc) {
	m.ackFn = f
}

func (m Message) Nack() error {
	if m.nackFn == nil {
		return fmt.Errorf("nack function is nil")
	}
	return m.nackFn()
}

func (m Message) SetNackFunc(f NackFunc) {
	m.nackFn = f
}
