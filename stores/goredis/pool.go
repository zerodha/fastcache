package goredis

import (
	"time"

	"github.com/go-redis/redis"
)

// NewPool returns a Redigo cachepool.
func NewPool(address string, password string, db int, maxActiv int, maxIdle int, timeout time.Duration) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:            address,
		Password:        password,
		DB:              db,
		PoolSize:        maxActiv,
		ReadTimeout:     timeout,
		WriteTimeout:    timeout,
		MinIdleConns:    maxIdle,
		MinRetryBackoff: 500 * time.Millisecond,
		MaxRetryBackoff: 2000 * time.Millisecond,
	})
}
