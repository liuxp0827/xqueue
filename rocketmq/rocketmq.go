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
	if q.producer == nil {
		return fmt.Errorf("producer no init")
	}
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
	if q.consumer == nil || q.consumerMessageChan == nil {
		return nil, fmt.Errorf("consumer no init")
	}
	select {
	case msg, ok := <-q.consumerMessageChan:
		if !ok {
			return nil, fmt.Errorf("closed chan")
		}
		return &Message{
			topic:     msg.Topic,
			groupId:   q.groupId(),
			tags:      strings.Split(msg.GetTags(), "||"),
			data:      msg.Body,
			messageId: msg.MsgId,
		}, nil
	case <-ctx.Done():
		return nil, nil
	}
}

type Message struct {
	topic     string
	groupId   string
	tags      []string
	data      []byte
	messageId string
}

func (m *Message) GetTopic() string {
	return m.topic
}

func (m *Message) SetTopic(topic string) {
	m.topic = topic
}

func (m *Message) SetTags(tags []string) {
	m.tags = tags
}

func (m *Message) GetTags() []string {
	return m.tags
}

func (m *Message) SetGroupId(gid string) {
	m.groupId = gid
}

func (m *Message) GetGroupId() string {
	return m.groupId
}

func (m *Message) SetData(data []byte) {
	m.data = data
}

func (m *Message) GetData() []byte {
	return m.data
}

func (m *Message) MessageId() string {
	return m.messageId
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
	EnableConsumer   bool                      `json:"enable_consumer"`
	EnableProducer   bool                      `json:"enable_producer"`
	Tags             []string                  `json:"tags"`
	EndPoint         string                    `json:"end_point"`
	AccessKey        string                    `json:"access_key"`
	SecretKey        string                    `json:"secret_key"`
	GroupId          string                    `json:"group_id"`
	InstanceId       string                    `json:"instance_id"`
	MessageModel     consumer.MessageModel     `json:"message_model"`
	ConsumeFromWhere consumer.ConsumeFromWhere `json:"consume_from_where"`
	WithAutoCommit   bool                      `json:"with_auto_commit"`
	WithOrder        bool                      `json:"with_order"`
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
func (p *Provider) QueueInit(ctx context.Context, config xqueue.QueueConfig) error {
	if p.queue == nil {
		err := json.Unmarshal([]byte(config.ProviderJsonConfig), &p.config)
		if err != nil {
			return err
		}
		var (
			cs rocketmq.PushConsumer = nil
			pd rocketmq.Producer     = nil
		)

		if p.config.EnableConsumer {
			consumerOptions := []consumer.Option{
				consumer.WithNameServer([]string{p.config.EndPoint}),
				consumer.WithCredentials(primitive.Credentials{
					AccessKey: p.config.AccessKey,
					SecretKey: p.config.SecretKey,
				}),
				consumer.WithGroupName(p.config.GroupId),
				consumer.WithNamespace(p.config.InstanceId),
				consumer.WithConsumerModel(p.config.MessageModel),
				consumer.WithConsumeFromWhere(p.config.ConsumeFromWhere),
				consumer.WithAutoCommit(p.config.WithAutoCommit),
				consumer.WithConsumerOrder(p.config.WithOrder),
			}
			cs, err = rocketmq.NewPushConsumer(consumerOptions...)
			if err != nil {
				return err
			}
		}

		if p.config.EnableProducer {
			producerOptions := []producer.Option{
				producer.WithNameServer([]string{p.config.EndPoint}),
				producer.WithCredentials(primitive.Credentials{
					AccessKey: p.config.AccessKey,
					SecretKey: p.config.SecretKey,
				}),
				producer.WithRetry(2),
				producer.WithGroupName(p.config.GroupId),
				producer.WithNamespace(p.config.InstanceId),
			}
			pd, err = rocketmq.NewProducer(producerOptions...)
			if err != nil {
				return err
			}
		}

		p.queue = &Queue{
			ctx:      ctx,
			config:   config,
			consumer: cs,
			producer: pd,
			lock:     sync.RWMutex{},
		}
		return p.queue.init()
	}
	return nil
}

func (p *Provider) Queue() (xqueue.Queue, error) {
	return p.queue, nil
}

func (p *Provider) QueueDestroy() error {
	if p.queue != nil {
		if p.queue.consumer != nil {
			err := p.queue.consumer.Shutdown()
			if err != nil {
				return err
			}
		}
		if p.queue.producer != nil {
			return p.queue.producer.Shutdown()
		}
	}
	return nil
}

func init() {
	xqueue.Register("rocketmq", rocketmqpder)
}
