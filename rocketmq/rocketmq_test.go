package rocketmq

import (
	"context"
	"github.com/liuxp0827/xqueue"
	"golang.org/x/sync/errgroup"
	"testing"
	"time"
)

var config = xqueue.QueueConfig{
	Topic:      "TOPIC_TEST",
	Tags:       []string{},
	InstanceId: "",
	GroupId:    "",
	ProviderJsonConfig: `{
    "enable_consumer": true,
    "enable_producer": true,
    "tags": [
    ],
    "end_point": "127.0.0.1:39876",
    "access_key": "",
    "secret_key": "",
    "group_id": "GID_TEST",
    "instance_id": "",
    "message_model": 0,
    "consume_from_where": 1,
    "with_auto_commit": true,
    "with_order": false
}`,
}

func initRocketmq() (*Provider, error) {
	p, err := xqueue.GetProvider("rocketmq")
	if err != nil {
		return nil, err
	}

	return p.(*Provider), nil
}

func TestQueue_Enqueue(t *testing.T) {
	var (
		eg, ctx = errgroup.WithContext(context.Background())
	)
	p, err := initRocketmq()
	if err != nil {
		t.Fatal(err)
	}

	err = p.QueueInit(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	q, err := p.Queue()
	if err != nil {
		t.Fatal(err)
	}

	eg.Go(func() error {
		return q.Enqueue(ctx, &Message{
			topic:     config.Topic,
			groupId:   "",
			tags:      config.Tags,
			data:      []byte("hello world!~~"),
			messageId: "",
		})
	})

	eg.Go(func() error {
		time.Sleep(1 * time.Second)
		msg, err := q.Dequeue(ctx)
		if err != nil {
			return err
		}
		t.Logf("data: %+v", string(msg.GetData()))
		return nil
	})
	err = eg.Wait()
	if err != nil {
		t.Fatal(err)
	}
}
