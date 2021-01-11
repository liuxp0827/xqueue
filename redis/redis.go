package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/go-redis/redis"
	"github.com/liuxp0827/xqueue"
)

type Queue struct {
	ctx    context.Context
	config xqueue.QueueConfig
	client *redis.Client
	lock   sync.RWMutex
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
	client := q.client.WithContext(ctx)
	return client.RPush(msg.GetTopic(), msg.GetData()).Err()
}

func (q *Queue) Dequeue(ctx context.Context) (xqueue.Message, error) {
	client := q.client.WithContext(ctx)
	data, err := client.LPop(q.topic()).Bytes()
	if err != nil {
		return nil, err
	}
	return &Message{
		Topic:   q.topic(),
		GroupId: q.groupId(),
		Tags:    q.tags(),
		Data:    data,
	}, nil
}

type Message struct {
	Topic   string
	GroupId string
	Tags    []string
	Data    []byte
}

func (rm *Message) GetTopic() string {
	return rm.Topic
}

func (rm *Message) SetTopic(topic string) {
	rm.Topic = topic
}

func (rm *Message) SetTags(tags []string) {
	rm.Tags = tags
}

func (rm *Message) GetTags() []string {
	return rm.Tags
}

func (rm *Message) SetGroupId(gid string) {
	rm.GroupId = gid
}

func (rm *Message) GetGroupId() string {
	return rm.GroupId
}

func (rm *Message) SetData(data []byte) {
	rm.Data = data
}

func (rm *Message) GetData() []byte {
	return rm.Data
}

func (rm *Message) MessageId() string {
	return ""
}
func (rm *Message) DequeueCount() int64 { // 已出队消费次数
	return 0
}

// Provider redis session provider
type Provider struct {
	config Config
	queue  *Queue
}

var redispder = &Provider{}

type Config struct {
	Ip       string
	Port     int
	PoolSize int
	Password string
	DbNum    int
	Timeout  int
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
		client := redis.NewClient(&redis.Options{
			Network:    "tcp",
			Addr:       fmt.Sprintf("%s:%d", p.config.Ip, p.config.Port),
			Password:   p.config.Password,
			DB:         p.config.DbNum,
			MaxRetries: 3,
			PoolSize:   p.config.PoolSize,
		})

		p.queue = &Queue{
			ctx:    ctx,
			config: config,
			client: client,
			lock:   sync.RWMutex{},
		}
	}
	return nil
}

func (p *Provider) Queue() (xqueue.Queue, error) {
	return p.queue, nil
}

func (p *Provider) QueueDestroy() error {
	return p.queue.client.Close()
}

func init() {
	xqueue.Register("redis", redispder)
}
