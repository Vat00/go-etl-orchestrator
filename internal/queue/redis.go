package queue

import (
	"time"

	"github.com/hibiken/asynq"
)

// NewRedisClient создаёт клиент для постановки задач (используется оркестратором)
func NewRedisClient(addr, password string, db int) *asynq.Client {
	return asynq.NewClient(asynq.RedisClientOpt{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
}

// NewRedisServer создаёт сервер для обработки задач (используется воркером)
func NewRedisServer(addr, password string, db int, concurrency int) *asynq.Server {
	return asynq.NewServer(
		asynq.RedisClientOpt{
			Addr:     addr,
			Password: password,
			DB:       db,
		},
		asynq.Config{
			Concurrency: concurrency,
			Queues: map[string]int{
				"critical": 6,
				"default":  3,
				"low":      1,
			},
			RetryDelayFunc: func(n int, e error, t *asynq.Task) time.Duration {
				return time.Duration(1<<uint(n)) * time.Second
			},
		},
	)
}
