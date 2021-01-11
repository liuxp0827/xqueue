package xqueue

import (
	"context"
	"fmt"
)

type Queue interface {
	Enqueue(ctx context.Context, msg Message) error
	Dequeue(ctx context.Context) (Message, error)
}

type Message interface {
	SetTopic(string)
	GetTopic() string
	SetTags([]string)
	GetTags() []string
	SetGroupId(string)
	GetGroupId() string
	SetData([]byte)
	GetData() []byte
	MessageId() string
	DequeueCount() int64 // 已出队消费次数
}

type Provider interface {
	QueueInit(ctx context.Context, config QueueConfig) error
	Queue() (Queue, error)
	QueueDestroy() error
}

type QueueConfig struct {
	Topic              string
	Tags               []string
	InstanceId         string
	GroupId            string
	ProviderJsonConfig string // 各种mq实现中间件的配置(redis,rocketmq,kafka等配置), JSON
}

var provides = make(map[string]Provider)

func Register(name string, provide Provider) {
	if provide == nil {
		panic("queue: Register provide is nil")
	}
	if _, dup := provides[name]; !dup {
		provides[name] = provide
	}
	provides[name] = provide
}

//GetProvider
func GetProvider(name string) (Provider, error) {
	provider, ok := provides[name]
	if !ok {
		return nil, fmt.Errorf("queue: unknown provide %q (forgotten import?)", name)
	}
	return provider, nil
}

// Manager contains Provider and its configuration.
type Manager struct {
	provider Provider
	config   QueueConfig
}

func NewManager(ctx context.Context, provideName string, config QueueConfig) (*Manager, error) {
	provider, ok := provides[provideName]
	if !ok {
		return nil, fmt.Errorf("queue: unknown provide %q (forgotten import?)", provideName)
	}

	err := provider.QueueInit(ctx, config)
	if err != nil {
		return nil, err
	}

	return &Manager{
		provider: provider,
		config:   config,
	}, nil
}

// GetProvider return current manager's provider
func (manager *Manager) GetProvider() Provider {
	return manager.provider
}
