package rocketmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/liuxp0827/xqueue"
	"golang.org/x/sync/errgroup"
)

type Queue struct {
	ctx                 context.Context
	config              xqueue.QueueConfig
	consumer            rocketmq.PushConsumer
	producer            rocketmq.Producer
	consumerMessageChan chan *primitive.MessageExt
	lock                sync.RWMutex
}

func (q *Queue) init() error {
	var (
		eg, _ = errgroup.WithContext(q.ctx)
	)
	if q.consumer != nil {
		eg.Go(func() error {
			q.consumerMessageChan = make(chan *primitive.MessageExt, 5)
			var selector consumer.MessageSelector
			if len(q.tags()) > 0 {
				selector = consumer.MessageSelector{
					Type:       consumer.TAG,
					Expression: strings.Join(q.tags(), "||"),
				}
			}
			err := q.consumer.Subscribe(q.topic(), selector, func(ctx context.Context, messages ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
				// 取到的消息放入管道，交给下游处理
				for _, msg := range messages {
					q.consumerMessageChan <- msg
				}
				return consumer.ConsumeSuccess, nil
			})
			if err != nil {
				return err
			}

			return q.consumer.Start()
		})
	}
	if q.producer != nil {
		eg.Go(func() error {
			return q.producer.Start()
		})
	}
	return eg.Wait()
}

func (q *Queue) topic() string {
	return q.config.Topic
}

func (q *Queue) groupId() string {
	return q.config.GroupId
}
func (q *Queue) tags() []string {
	return q.config.Tags
}

func (q *Queue) Enqueue(ctx context.Context, msg xqueue.Message) error {

	m := &primitive.Message{
		Topic: q.topic(),
		Body:  msg.GetData(),
	}
	if len(q.tags()) > 0 {
		tags := strings.Join(q.tags(), "||")
		m.WithTag(tags)
	}
	_, err := q.producer.SendSync(ctx, m)
	return err
}

func (q *Queue) Dequeue(ctx context.Context) (xqueue.Message, error) {
	msg, ok := <-q.consumerMessageChan
	if !ok {
		return nil, fmt.Errorf("closed chan")
	}
	return &Message{
		Topic:   msg.Topic,
		GroupId: q.groupId(),
		Tags:    strings.Split(msg.GetTags(), "||"),
		Data:    msg.Body,
	}, nil
}

type Message struct {
	Topic   string
	GroupId string
	Tags    []string
	Data    []byte
}

func (m *Message) GetTopic() string {
	return m.Topic
}

func (m *Message) SetTopic(topic string) {
	m.Topic = topic
}

func (m *Message) SetTags(tags []string) {
	m.Tags = tags
}

func (m *Message) GetTags() []string {
	return m.Tags
}

func (m *Message) SetGroupId(gid string) {
	m.GroupId = gid
}

func (m *Message) GetGroupId() string {
	return m.GroupId
}

func (m *Message) SetData(data []byte) {
	m.Data = data
}

func (m *Message) GetData() []byte {
	return m.Data
}

func (m *Message) MessageId() string {
	return ""
}
func (m *Message) DequeueCount() int64 { // 已出队消费次数
	return 0
}

// Provider redis session provider
type Provider struct {
	config Config
	queue  *Queue
}

var rocketmqpder = &Provider{}

type Config struct {
	EnableConsumer   bool
	EnableProducer   bool
	Tags             []string
	EndPoint         string
	AccessKey        string
	SecretKey        string
	GroupId          string
	InstanceId       string
	MessageModel     consumer.MessageModel
	ConsumeFromWhere consumer.ConsumeFromWhere
	WithAutoCommit   bool
	WithOrder        bool
}

/*
	{
		"ip": "127.0.0.1",
		"port": 6379,
		"pool_size": 100,
		"password": "abc",
		"db_num": 0,
		"timeout": 10
	}
*/
func (rp *Provider) QueueInit(ctx context.Context, config xqueue.QueueConfig) error {
	if rp.queue == nil {
		err := json.Unmarshal([]byte(config.ProviderJsonConfig), &rp.config)
		if err != nil {
			return err
		}
		var (
			c rocketmq.PushConsumer = nil
			p rocketmq.Producer     = nil
		)

		if rp.config.EnableConsumer {
			consumerOptions := []consumer.Option{
				consumer.WithNameServer([]string{rp.config.EndPoint}),
				consumer.WithCredentials(primitive.Credentials{
					AccessKey: rp.config.AccessKey,
					SecretKey: rp.config.SecretKey,
				}),
				consumer.WithGroupName(rp.config.GroupId),
				consumer.WithNamespace(rp.config.InstanceId),
				consumer.WithConsumerModel(rp.config.MessageModel),
				consumer.WithConsumeFromWhere(rp.config.ConsumeFromWhere),
				consumer.WithAutoCommit(rp.config.WithAutoCommit),
				consumer.WithConsumerOrder(rp.config.WithOrder),
			}
			c, err = rocketmq.NewPushConsumer(consumerOptions...)
			if err != nil {
				return err
			}
		}

		if rp.config.EnableProducer {
			producerOptions := []producer.Option{
				producer.WithNameServer([]string{rp.config.EndPoint}),
				producer.WithCredentials(primitive.Credentials{
					AccessKey: rp.config.AccessKey,
					SecretKey: rp.config.SecretKey,
				}),
				producer.WithRetry(2),
				producer.WithGroupName(rp.config.GroupId),
				producer.WithNamespace(rp.config.InstanceId),
			}
			p, err = rocketmq.NewProducer(producerOptions...)
			if err != nil {
				return err
			}
		}

		rp.queue = &Queue{
			ctx:      ctx,
			config:   config,
			consumer: c,
			producer: p,
			lock:     sync.RWMutex{},
		}
		return rp.queue.init()
	}
	return nil
}

func (rp *Provider) Queue() (xqueue.Queue, error) {
	return rp.queue, nil
}

func (rp *Provider) QueueDestroy() error {
	if rp.queue != nil {
		if rp.queue.consumer != nil {
			err := rp.queue.consumer.Shutdown()
			if err != nil {
				return err
			}
		}
		if rp.queue.producer != nil {
			return rp.queue.producer.Shutdown()
		}
	}
	return nil
}

func init() {
	xqueue.Register("rocketmq", rocketmqpder)
}
