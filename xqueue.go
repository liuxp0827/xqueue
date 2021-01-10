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
	SetData([]byte)
	GetData() []byte
	MessageId() string
}

type Provider interface {
	QueueInit(config string) error
	Queue() (Queue, error)
	QueueDestroy() error
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
	config   string
}

func NewManager(provideName string, config string) (*Manager, error) {
	provider, ok := provides[provideName]
	if !ok {
		return nil, fmt.Errorf("queue: unknown provide %q (forgotten import?)", provideName)
	}

	err := provider.QueueInit(config)
	if err != nil {
		return nil, err
	}

	return &Manager{
		provider,
		config,
	}, nil
}

// GetProvider return current manager's provider
func (manager *Manager) GetProvider() Provider {
	return manager.provider
}
