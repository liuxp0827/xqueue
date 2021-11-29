package xqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/liuxp0827/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type ProducerProvider interface {
	Name() string
	Publish(ctx context.Context, topic string, data []byte, opts ...PublishOption) (id string, err error)
	Close() error
}

type Client struct {
	provider ProducerProvider
	tracer   trace.Tracer
	log      *log.Helper
}

func NewClient(provider ProducerProvider, logger log.Logger) *Client {
	name := "producer"
	if pname := provider.Name(); len(pname) > 0 {
		name = fmt.Sprintf("%s.producer", pname)
	}
	return &Client{
		provider: provider,
		tracer:   otel.Tracer(name),
		log: log.NewHelper(log.With(
			logger,
			"msg_id", MessageID(),
			"topic", Topic(),
		)),
	}
}

func (c *Client) Send(ctx context.Context, topic string, data []byte) error {
	var (
		span  trace.Span
		attrs = make([]attribute.KeyValue, 0)
		tn    = time.Now()
	)

	ctx, span = c.tracer.Start(ctx, topic, trace.WithSpanKind(trace.SpanKindProducer))
	defer func() {
		span.SetAttributes(attrs...)
		span.End()
	}()

	ctx = TopicContext(ctx, topic)

	attrs = append(attrs, attribute.Key("topic").String(topic))
	attrs = append(attrs, attribute.Key("payload").String(string(data)))

	c.log.WithContext(ctx).Infof(string(data))
	id, err := c.provider.Publish(ctx, topic, data)
	if err != nil {
		span.RecordError(err, trace.WithTimestamp(tn))
		c.log.WithContext(ctx).Errorf("Send: %v", err)
		return err
	}
	ctx = MessageIDContext(ctx, id)
	attrs = append(attrs, attribute.Key("message_id").String(id))
	c.log.WithContext(ctx).Infof("send success")
	return nil
}
