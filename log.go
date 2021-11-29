package xqueue

import (
	"context"
	"fmt"

	"github.com/liuxp0827/log"
)

type messageIDKey struct{}
type redeliveryCountKey struct{}
type topicKey struct{}

// MessageID returns a MessageID valuer.
func MessageID() log.Valuer {
	return func(ctx context.Context) interface{} {
		if ctx != nil {
			if msgID, ok := ctx.Value(messageIDKey{}).(string); ok {
				return msgID
			}
		}
		return ""
	}
}

func MessageIDContext(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, messageIDKey{}, id)
}

// RedeliveryCount returns a RedeliveryCount valuer.
func RedeliveryCount() log.Valuer {
	return func(ctx context.Context) interface{} {
		if ctx != nil {
			if count, ok := ctx.Value(redeliveryCountKey{}).(int); ok {
				return fmt.Sprintf("%d", count)
			}
		}
		return ""
	}
}

func RedeliveryCountContext(ctx context.Context, count int) context.Context {
	return context.WithValue(ctx, redeliveryCountKey{}, count)
}

// Topic returns a Topic valuer.
func Topic() log.Valuer {
	return func(ctx context.Context) interface{} {
		if ctx != nil {
			if topic, ok := ctx.Value(topicKey{}).(string); ok {
				return topic
			}
		}
		return ""
	}
}

func TopicContext(ctx context.Context, topic string) context.Context {
	return context.WithValue(ctx, topicKey{}, topic)
}
