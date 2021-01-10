package redis

import (
	"context"
	"encoding/json"
	"github.com/gomodule/redigo/redis"
	"github.com/liuxp0827/xqueue"
	"sync"
)

type RedisQueue struct {
	p    *redis.Pool
	lock sync.RWMutex
}

func (rq *RedisQueue) Enqueue(ctx context.Context, msg xqueue.Message) error {
	return nil
}

func (rq *RedisQueue) Dequeue(ctx context.Context) (xqueue.Message, error) {
	return &RedisMessage{}, nil
}

type RedisMessage struct {
	Topic string
	Tags  []string
	Data  []byte
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

func (rm *RedisMessage) SetData(data []byte) {
	rm.Data = data
}

func (rm *RedisMessage) GetData() []byte {
	return rm.Data
}

func (rm *RedisMessage) MessageId() string {
	return ""
}

// Provider redis session provider
type Provider struct {
	config      Config
	maxlifetime int64
	savePath    string
	poolsize    int
	password    string
	dbNum       int
	pool        *redis.Pool
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
func (rp *Provider) QueueInit(config string) error {
	err := json.Unmarshal([]byte(config), &rp.config)
	if err != nil {
		return err
	}
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", rp.savePath)
			if err != nil {
				return nil, err
			}
			if rp.password != "" {
				if _, err = c.Do("AUTH", rp.password); err != nil {
					_ = c.Close()
					return nil, err
				}
			}
			if rp.dbNum > 0 {
				_, err = c.Do("SELECT", rp.dbNum)
				if err != nil {
					_ = c.Close()
					return nil, err
				}
			}
			return c, err
		},
		MaxIdle: rp.poolsize,
	}
	if rp.queue == nil {
		rp.queue = &RedisQueue{
			p:    nil,
			lock: sync.RWMutex{},
		}
	}
	rp.queue.p = pool

	return nil
}

func (rp *Provider) Queue() (xqueue.Queue, error) {
	return rp.queue, nil
}

func (rp *Provider) QueueDestroy() error {
	return rp.queue.p.Close()
}

func init() {
	xqueue.Register("redis", redispder)
}
