package xqueue

import "time"

type Options struct{}

type Option func(o *Options)

// PublishOptions contains all the options which can be provided when publishing an event
type PublishOptions struct {
	// Metadata contains any keys which can be used to query the data, for example a customer id
	Metadata map[string]string
	// Timestamp to set for the event, if the timestamp is a zero value, the current time will be used
	Timestamp time.Time
}

// PublishOption sets attributes on PublishOptions
type PublishOption func(o *PublishOptions)

// WithMetadata sets the Metadata field on PublishOptions
func WithMetadata(md map[string]string) PublishOption {
	return func(o *PublishOptions) {
		o.Metadata = md
	}
}

// WithTimestamp sets the timestamp field on PublishOptions
func WithTimestamp(t time.Time) PublishOption {
	return func(o *PublishOptions) {
		o.Timestamp = t
	}
}

// ConsumeOptions contains all the options which can be provided when subscribing to a topic
type ConsumeOptions struct {
	WorkersSize int

	// Group is the name of the consumer group, if two consumers have the same group the events
	// are distributed between them
	Group string
	// RetryLimit indicates number of times a message is retried
	RetryLimit int
	// CustomRetries indicates whether to use RetryLimit
	CustomRetries bool
}

// ConsumeOption sets attributes on ConsumeOptions
type ConsumeOption func(o *ConsumeOptions)

// WithGroup sets the consumer group to be part of when consuming events
func WithGroup(q string) ConsumeOption {
	return func(o *ConsumeOptions) {
		o.Group = q
	}
}

// WithRetryLimit sets the RetryLimit field on ConsumeOptions.
// Set to -1 for infinite retries (default)
func WithRetryLimit(retries int) ConsumeOption {
	return func(o *ConsumeOptions) {
		o.RetryLimit = retries
		o.CustomRetries = true
	}
}

func (s ConsumeOptions) GetRetryLimit() int {
	if !s.CustomRetries {
		return -1
	}
	return s.RetryLimit
}
