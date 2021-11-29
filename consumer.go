package xqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/liuxp0827/log"
	"github.com/panjf2000/ants/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

type ConsumerFunc func(context.Context, []byte) (ResultCode, error)

type Consumer interface {
	Topic() string
	ConsumerFunc() ConsumerFunc
}

type ConsumerProvider interface {
	Name() string
	Consume(ctx context.Context, topic string, opts ...ConsumeOption) (<-chan Message, error)
	Close() error
}

type consumer struct {
	Consumer
	provider ConsumerProvider
	msg      <-chan Message
	ctx      context.Context
	stop     func()
}

type Server struct {
	provider  ConsumerProvider
	consumers map[string]consumer
	runners   []*consumer
	eg        *errgroup.Group
	ctx       context.Context
	tracer    trace.Tracer
	log       *log.Helper
}

func NewServer(provider ConsumerProvider, logger log.Logger) *Server {
	eg, ctx := errgroup.WithContext(context.Background())

	name := "consumer"
	if pname := provider.Name(); len(pname) > 0 {
		name = fmt.Sprintf("%s.consumer", pname)
	}

	return &Server{
		provider:  provider,
		consumers: make(map[string]consumer),
		runners:   make([]*consumer, 0),
		eg:        eg,
		ctx:       ctx,
		tracer:    otel.Tracer(name),
		log: log.NewHelper(log.With(
			logger,
			"msg_id", MessageID(),
			"redelivery_count", RedeliveryCount(),
			"topic", Topic(),
		)),
	}
}

func (s *Server) Register(c Consumer) {
	s.consumers[c.Topic()] = consumer{
		Consumer: c,
	}
}

func (s *Server) Run(topics []string) (err error) {
	if len(topics) == 0 {
		return fmt.Errorf("topic is empty")
	}
	if len(s.runners) == 0 {
		for _, topic := range topics {
			if r, exist := s.consumers[topic]; !exist {
				return fmt.Errorf("topic %s is not existed", topic)
			} else {
				ctx, cancel := context.WithCancel(s.ctx)
				r.ctx = ctx
				r.stop = cancel
				s.log.WithContext(ctx).Infof("subscribe topic: %v", topic)
				r.msg, err = s.provider.Consume(ctx, topic)
				if err != nil {
					s.log.WithContext(ctx).Error(err)
					return err
				}
				s.runners = append(s.runners, &r)
			}
		}
	}
	return nil
}

func (s *Server) Start(ctx context.Context) (err error) {
	if len(s.runners) == 0 {
		return fmt.Errorf("no pick topic")
	}
	for i := 0; i < len(s.runners); i++ {
		r := s.runners[i]
		s.eg.Go(func() error {
			s.log.WithContext(ctx).Infof("[%s consumer job] server starting, topic: [%v]", s.provider.Name(), r.Topic())
			return s.run(r)
		})
	}
	return s.eg.Wait()
}

func (s *Server) Stop(ctx context.Context) error {
	s.log.WithContext(ctx).Info("[job] server stopping")
	for _, r := range s.runners {
		s.log.WithContext(ctx).Infof("[%s consumer job] server stopping, topic: [%v]", s.provider.Name(), r.Topic())
		r.stop()
	}
	return s.provider.Close()
}

func (s *Server) run(r *consumer) error {
	pool, err := ants.NewPool(
		DefaultWorkersPoolSize,
		ants.WithOptions(ants.Options{
			ExpiryDuration: ExpiryDuration,
			Nonblocking:    Nonblocking,
		}))
	if err != nil {
		s.log.Error(err)
		return err
	}

	go func() {
		s.log.Infof("[%s consumer job] - [%s] loop starting...", s.provider.Name(), r.Topic())
		for {
			select {
			case msg := <-r.msg:
				errPool := pool.Submit(func() {
					var (
						ctx   = context.Background()
						span  trace.Span
						attrs = make([]attribute.KeyValue, 0)
						tn    = time.Now()
					)

					if msg.Ctx != nil {
						ctx = msg.Ctx
					}

					ctx, span = s.tracer.Start(ctx, fmt.Sprintf("%s %s", s.provider.Name(), r.Topic()), trace.WithSpanKind(trace.SpanKindConsumer))
					defer func() {
						span.SetAttributes(attrs...)
						span.End()
					}()

					ctx = MessageIDContext(ctx, msg.ID)
					ctx = RedeliveryCountContext(ctx, msg.RedeliveryCount)
					ctx = TopicContext(ctx, r.Topic())

					attrs = append(attrs, attribute.Key("topic").String(msg.Topic))
					attrs = append(attrs, attribute.Key("payload").String(string(msg.Payload)))
					attrs = append(attrs, attribute.Key("message_id").String(msg.ID))
					attrs = append(attrs, attribute.Key("redelivery_count").Int(msg.RedeliveryCount))

					s.log.WithContext(ctx).Info(string(msg.Payload))

					result, err := r.ConsumerFunc()(ctx, msg.Payload)
					if err != nil {
						span.RecordError(err, trace.WithTimestamp(tn))
						s.log.WithContext(ctx).Error(err)
						err = msg.Nack()
						if err != nil {
							s.log.WithContext(ctx).Error(err)
						}
						attrs = append(attrs, attribute.Key("nack.message_id").String(msg.ID))
						return
					}

					s.log.WithContext(ctx).Infof("result: %v", result.String())
					attrs = append(attrs, attribute.Key("consume.result").String(result.String()))

					switch result {
					case ResultInvalidMessage, ResultRetryOverLimit, ResultSuccess:
						_ = msg.Ack()
						attrs = append(attrs, attribute.Key("ack.message_id").String(msg.ID))
					default:
						_ = msg.Nack()
						attrs = append(attrs, attribute.Key("nack.message_id").String(msg.ID))
					}
				})
				if errPool != nil {
					s.log.Error(errPool)
				}
			case <-r.ctx.Done():
				s.log.Infof("[%s consumer job] loop stopping, topic: [%v]", s.provider.Name(), r.Topic())
				return
			case <-s.ctx.Done():
				s.log.Infof("[%s consumer job] loop stopping, topic: [%v]", s.provider.Name(), r.Topic())
				return
			}
		}
	}()
	return nil
}
