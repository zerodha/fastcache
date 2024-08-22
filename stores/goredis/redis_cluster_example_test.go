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
		Addrs:           []string{"localhost:6379", "localhost:6380", "localhost:6381", "localhost:6382", "localhost:6383", "localhost:6384"},
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
	for i := 0; i < 5; i++ {
		_ = cl.Put("namespace", "group", fmt.Sprintf("/test/endpoint%d", i), fastcache.Item{
			ETag:        "etag",
			ContentType: "content_type",
			Blob:        []byte("{}"),
		}, time.Second*3)
	}

	time.Sleep(time.Millisecond * 200)

	for i := 0; i < 5; i++ {
		item, err := cl.Get("namespace", "group", fmt.Sprintf("/test/endpoint%d", i))
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println(item)
	}

	// Output:
	// {etag content_type [123 125]}
	// {etag content_type [123 125]}
	// {etag content_type [123 125]}
	// {etag content_type [123 125]}
	// {etag content_type [123 125]}
}
