package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/go-redis/redis"
	"github.com/liuxp0827/xqueue"
)

type RedisQueue struct {
	config xqueue.QueueConfig
	client *redis.Client
	lock   sync.RWMutex
}

func (rq *RedisQueue) topic() string {
	return rq.config.Topic
}

func (rq *RedisQueue) groupId() string {
	return rq.config.GroupId
}
func (rq *RedisQueue) tags() []string {
	return rq.config.Tags
}

func (rq *RedisQueue) Enqueue(ctx context.Context, msg xqueue.Message) error {
	client := rq.client.WithContext(ctx)
	return client.RPush(msg.GetTopic(), msg.GetData()).Err()
}

func (rq *RedisQueue) Dequeue(ctx context.Context) (xqueue.Message, error) {
	client := rq.client.WithContext(ctx)
	data, err := client.LPop(rq.topic()).Bytes()
	if err != nil {
		return nil, err
	}
	return &RedisMessage{
		Topic:   rq.topic(),
		GroupId: rq.groupId(),
		Tags:    rq.tags(),
		Data:    data,
	}, nil
}

type RedisMessage struct {
	Topic   string
	GroupId string
	Tags    []string
	Data    []byte
}

func (rm *RedisMessage) GetTopic() string {
	return rm.Topic
}

func (rm *RedisMessage) SetTopic(topic string) {
	rm.Topic = topic
}

func (rm *RedisMessage) SetTags(tags []string) {
	rm.Tags = tags
}

func (rm *RedisMessage) GetTags() []string {
	return rm.Tags
}

func (rm *RedisMessage) SetGroupId(gid string) {
	rm.GroupId = gid
}

func (rm *RedisMessage) GetGroupId() string {
	return rm.GroupId
}

func (rm *RedisMessage) SetData(data []byte) {
	rm.Data = data
}

func (rm *RedisMessage) GetData() []byte {
	return rm.Data
}

func (rm *RedisMessage) MessageId() string {
	return ""
}
func (rm *RedisMessage) DequeueCount() int64 { // 已出队消费次数
	return 0
}

// Provider redis session provider
type Provider struct {
	config      Config
	maxlifetime int64
	savePath    string
	poolsize    int
	password    string
	dbNum       int
	queue       *RedisQueue
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
func (rp *Provider) QueueInit(config xqueue.QueueConfig) error {
	if rp.queue == nil {
		err := json.Unmarshal([]byte(config.ProviderJsonConfig), &rp.config)
		if err != nil {
			return err
		}
		client := redis.NewClient(&redis.Options{
			Network:    "tcp",
			Addr:       fmt.Sprintf("%s:%d", rp.config.Ip, rp.config.Port),
			Password:   rp.config.Password,
			DB:         rp.config.DbNum,
			MaxRetries: 3,
			PoolSize:   rp.poolsize,
		})

		rp.queue = &RedisQueue{
			config: config,
			client: client,
			lock:   sync.RWMutex{},
		}
	}
	return nil
}

func (rp *Provider) Queue() (xqueue.Queue, error) {
	return rp.queue, nil
}

func (rp *Provider) QueueDestroy() error {
	return rp.queue.client.Close()
}

func init() {
	xqueue.Register("redis", redispder)
}
