package xqueue

import (
	"time"
)

type ResultCode int

func (r ResultCode) String() string {
	return resultStr[r]
}

const (
	ResultFailed         ResultCode = iota + 1 // 消费失败, 需要重传, 不进行ACK
	ResultInvalidMessage                       // 消费负载异常, 不需要重传, 进行ACK
	ResultRetryOverLimit                       // 重传消费超过限制次数, 不再重传, 进行ACK
	ResultSuccess                              // 消费成功, 不再重传, 进行ACK
)

var resultStr = map[ResultCode]string{
	ResultInvalidMessage: "invalid message",
	ResultFailed:         "fail",
	ResultRetryOverLimit: "retry over limit",
	ResultSuccess:        "success",
}

const (
	DefaultWorkersPoolSize = 1 << 2 // 默认工作池大小.

	ExpiryDuration = 10 * time.Second // 清理回收周期.

	Nonblocking = false // 阻塞, 无可用协程则等待.
)

type Provider interface {
	ConsumerProvider
	ProducerProvider
}
