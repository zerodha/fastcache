package goredis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zerodha/fastcache/v4"
)

func ExampleNew() {
	pool := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:           []string{"0.0.0.0:6379", "0.0.0.0:6380", "0.0.0.0:6381", "0.0.0.0:6382", "0.0.0.0:6383", "0.0.0.0:6384"},
		MinIdleConns:    1,
		MaxActiveConns:  10,
		DialTimeout:     time.Second * 1,
		ReadTimeout:     time.Second * 1,
		WriteTimeout:    time.Second * 1,
		ConnMaxIdleTime: time.Second * 1,
		RouteRandomly:   true,
	})

	pingCtx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	err := pool.Ping(pingCtx).Err()
	if err != nil {
		fmt.Println(err)
		return
	}

	cl := New(Config{
		Prefix:             "TEST:",
		Async:              true,
		AsyncMaxCommitSize: 5,
		AsyncBufSize:       100,
		AsyncBufTimeout:    100 * time.Millisecond,
	}, pool)

	// Perform async writes
	for i := 0; i < 20; i++ {
		_ = cl.Put("namespace", "group", fmt.Sprintf("/test/endpoint%d", i), fastcache.Item{
			ETag:        "etag",
			ContentType: "content_type",
			Blob:        []byte("{}"),
		}, time.Second*3)
	}

	for i := 0; i < 20; i++ {
		item, err := cl.Get("namespace", "group", fmt.Sprintf("/test/endpoint%d", i))
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println(item)
	}

	// Output:
	// {etag content_type [123 125]}
}
