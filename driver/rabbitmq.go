package driver

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/liuxp0827/log"
	"github.com/liuxp0827/xqueue"
	"github.com/streadway/amqp"
)

type RabbitmqConsumer struct {
	client  *amqp.Channel
	topic   string
	channel <-chan amqp.Delivery
	msg     chan xqueue.Message
	ctx     context.Context
	stop    func()
}

type RabbitmqProducer struct {
	client *amqp.Channel
	q      amqp.Queue
	topic  string
}

type Rabbitmq struct {
	options *xqueue.ConsumeOptions

	consumers map[string]*RabbitmqConsumer
	producers map[string]*RabbitmqProducer
	conn      *amqp.Connection
	log       *log.Helper
}

type RabbitmqConfig struct {
	Host            string
	Port            uint32
	Password        string
	Username, VHost string
}

func NewRabbitmq(cfg RabbitmqConfig, logger log.Logger) (xqueue.Provider, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.VHost,
	))
	if err != nil {
		return nil, err
	}
	return &Rabbitmq{
		options:   &xqueue.ConsumeOptions{WorkersSize: xqueue.DefaultWorkersPoolSize},
		conn:      conn,
		consumers: make(map[string]*RabbitmqConsumer),
		producers: make(map[string]*RabbitmqProducer),
		log:       log.NewHelper(logger),
	}, nil

}

func (r *Rabbitmq) Publish(ctx context.Context, topic string, data []byte, opts ...xqueue.PublishOption) (id string, err error) {
	uid, _ := uuid.NewUUID()
	id = uid.String()

	p, err := r.producer(topic)
	if err != nil {
		r.log.WithContext(ctx).Errorf("Publish: %v", err)
		return id, err
	}
	err = p.client.Publish(
		"",
		p.q.Name,
		false,
		false, amqp.Publishing{
			ContentType: "text/plain",
			MessageId:   id,
			Body:        data,
		},
	)
	return id, err
}

func (r *Rabbitmq) producer(topic string) (*RabbitmqProducer, error) {
	if len(topic) == 0 {
		return nil, fmt.Errorf("empty topic")
	}
	p, ok := r.producers[topic]
	if !ok {
		p, ok = r.producers[topic]
		if !ok {
			client, err := r.conn.Channel()
			if err != nil {
				return nil, err
			}
			q, err := client.QueueDeclare(
				topic, // name
				true,  // durable
				false, // delete when unused
				false, // exclusive
				false, // no-wait
				nil,   // arguments
			)
			if err != nil {
				return nil, err
			}
			p = &RabbitmqProducer{
				client: client,
				q:      q,
				topic:  topic,
			}
			r.producers[topic] = p
		}
	}
	return p, nil
}

func (r *Rabbitmq) Name() string {
	return "pulsar"
}

func (r *Rabbitmq) Consume(ctx context.Context, topic string, opts ...xqueue.ConsumeOption) (<-chan xqueue.Message, error) {
	for _, o := range opts {
		o(r.options)
	}
	ctx, stop := context.WithCancel(ctx)
	rc := &RabbitmqConsumer{
		topic: topic,
		ctx:   ctx,
		stop:  stop,
	}

	client, err := r.conn.Channel()
	if err != nil {
		r.log.WithContext(ctx).Error(err)
		return nil, err
	}

	q, err := client.QueueDeclare(
		topic, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		r.log.WithContext(ctx).Error(err)
		return nil, err
	}

	err = client.Qos(1, 0, false)
	if err != nil {
		r.log.WithContext(ctx).Error(err)
		return nil, err
	}

	channel, err := client.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		r.log.WithContext(ctx).Error(err)
		return nil, err
	}

	msg := make(chan xqueue.Message, r.options.WorkersSize)
	rc.client = client
	rc.channel = channel
	rc.msg = msg
	r.consumers[topic] = rc

	go r.loop(r.consumers[topic])
	return msg, nil
}

func (r *Rabbitmq) loop(c *RabbitmqConsumer) {
	for {
		select {
		case msg := <-c.channel:
			ctx := context.Background()
			mid := msg.MessageId
			r.log.WithContext(ctx).Infow(
				"mid", mid,
				"raw", string(msg.Body))
			uid, _ := uuid.NewUUID()
			m := xqueue.Message{
				Ctx:             ctx,
				ID:              mid,
				UUID:            uid.String(),
				Payload:         msg.Body,
				Topic:           c.topic,
				Timestamp:       msg.Timestamp,
				Metadata:        map[string]interface{}{},
				RedeliveryCount: int(msg.MessageCount),
			}
			r.log.WithContext(ctx).Info(m.String())
			m.SetAckFunc(func() error {
				return msg.Ack(false)
			})
			m.SetNackFunc(func() error {
				return msg.Nack(false, false)
			})
			c.msg <- m
		case <-c.ctx.Done():
			return
		}
	}
}

func (r *Rabbitmq) Close() error {
	for _, c := range r.consumers {
		err := c.client.Close()
		if err != nil {
			return err
		}
		if c.stop != nil {
			c.stop()
		}
	}
	return nil
}
