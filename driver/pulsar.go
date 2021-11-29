package driver

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/liuxp0827/log"
	"github.com/liuxp0827/xqueue"
)

type PulsarConsumer struct {
	consumer pulsar.Consumer
	topic    string
	channel  chan pulsar.ConsumerMessage
	msg      chan xqueue.Message
	ctx      context.Context
	stop     func()
}

type PulsarProducer struct {
	producer pulsar.Producer
	topic    string
}

type Pulsar struct {
	options *xqueue.ConsumeOptions

	namespace string

	consumers map[string]*PulsarConsumer
	producers map[string]*PulsarProducer
	client    pulsar.Client
	log       *log.Helper
}

type PulsarConfig struct {
	Addr, Token, Namespace string
}

func NewPulsar(cfg PulsarConfig, logger log.Logger) (xqueue.Provider, error) {
	var (
		auth pulsar.Authentication = nil
		lg                         = log.NewHelper(logger)
	)
	if len(cfg.Token) > 0 {
		lg.Infof("token: %v", cfg.Token)
		auth = pulsar.NewAuthenticationToken(cfg.Token)
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            cfg.Addr,
		Authentication: auth,
	})
	if err != nil {
		return nil, err
	}
	return &Pulsar{
		options:   &xqueue.ConsumeOptions{WorkersSize: xqueue.DefaultWorkersPoolSize},
		namespace: cfg.Namespace,
		consumers: make(map[string]*PulsarConsumer),
		producers: make(map[string]*PulsarProducer),
		client:    client,
		log:       lg,
	}, nil

}

func (p *Pulsar) Publish(ctx context.Context, topic string, data []byte, opts ...xqueue.PublishOption) (id string, err error) {
	if len(p.namespace) > 0 {
		p.namespace = strings.Trim(p.namespace, "/")
		topic = path.Join(p.namespace, topic)
	}
	producer, err := p.producer(topic)
	if err != nil {
		p.log.WithContext(ctx).Errorf("Publish: %v", err)
		return "", err
	}
	msgID, err := producer.producer.Send(ctx, &pulsar.ProducerMessage{Payload: data})
	if err != nil {
		p.log.WithContext(ctx).Errorf("Publish: %v", err)
		return "", err
	}
	id = MD5(msgID.Serialize())
	p.log.WithContext(ctx).Infow("id", id, "data", string(data))
	return id, nil
}

func (p *Pulsar) producer(topic string) (*PulsarProducer, error) {
	if len(topic) == 0 {
		return nil, fmt.Errorf("empty topic")
	}
	producer, ok := p.producers[topic]
	if !ok {
		producer, ok = p.producers[topic]
		if !ok {
			prod, err := p.client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
			if err != nil {

				return nil, err
			}
			producer = &PulsarProducer{
				producer: prod,
				topic:    topic,
			}
			p.producers[topic] = producer
		}
	}
	return producer, nil
}

func (p *Pulsar) Name() string {
	return "pulsar"
}

func (p *Pulsar) Consume(ctx context.Context, topic string, opts ...xqueue.ConsumeOption) (<-chan xqueue.Message, error) {
	for _, o := range opts {
		o(p.options)
	}

	if len(p.namespace) > 0 {
		p.namespace = strings.Trim(p.namespace, "/")
		topic = path.Join(p.namespace, topic)
	}
	ctx, stop := context.WithCancel(ctx)

	channel := make(chan pulsar.ConsumerMessage, p.options.WorkersSize)
	msg := make(chan xqueue.Message, p.options.WorkersSize)

	pc := &PulsarConsumer{
		topic:   topic,
		channel: channel,
		msg:     msg,
		ctx:     ctx,
		stop:    stop,
	}

	consumer, err := p.client.Subscribe(pulsar.ConsumerOptions{
		DLQ: &pulsar.DLQPolicy{
			MaxDeliveries:    uint32(p.options.RetryLimit),
			DeadLetterTopic:  topic + "-DLQ",
			RetryLetterTopic: topic + "-RETRY",
		},
		Topic:               topic,
		SubscriptionName:    p.options.Group,
		MessageChannel:      channel,
		Type:                pulsar.Shared,
		NackRedeliveryDelay: 30 * time.Second,
	})
	if err != nil {
		p.log.WithContext(ctx).Error(err)
		return nil, err
	}

	pc.consumer = consumer
	p.consumers[topic] = pc
	go p.loop(pc)
	return msg, nil
}

func (p *Pulsar) loop(c *PulsarConsumer) {
	for {
		select {
		case msg := <-c.channel:
			ctx := context.Background()
			mid := MD5(msg.ID().Serialize())
			p.log.WithContext(ctx).Infow(
				"topic", c.topic,
				"mid", mid,
				"raw", string(msg.Payload()))
			uid, _ := uuid.NewUUID()
			m := xqueue.Message{
				Ctx:             ctx,
				ID:              mid,
				UUID:            uid.String(),
				Payload:         msg.Payload(),
				Topic:           msg.Topic(),
				Timestamp:       msg.EventTime(),
				Metadata:        map[string]interface{}{},
				RedeliveryCount: int(msg.RedeliveryCount()),
			}
			p.log.WithContext(ctx).Info(m.String())
			m.SetAckFunc(func() error {
				if c.consumer != nil {
					c.consumer.Ack(msg)
					return nil
				}
				return fmt.Errorf("can not find consumer with topic[%s]", msg.Topic())
			})
			m.SetNackFunc(func() error {
				if c.consumer != nil {
					c.consumer.Nack(msg)
					return nil
				}
				return fmt.Errorf("can not find consumer with topic[%s]", msg.Topic())
			})
			c.msg <- m
		case <-c.ctx.Done():
			return
		}
	}
}

func (p *Pulsar) Close() error {
	if p.consumers != nil {
		for _, consumer := range p.consumers {
			consumer.consumer.Close()
			if consumer.stop != nil {
				consumer.stop()
			}
		}
	}
	if p.producers != nil {
		for _, producer := range p.producers {
			producer.producer.Close()
		}
	}
	if p.client != nil {
		p.client.Close()
	}
	return nil
}

func MD5(b []byte) string {
	m := md5.New()
	_, _ = m.Write(b)
	return hex.EncodeToString(m.Sum(nil))
}
