package goredis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/zerodha/fastcache/v4"
)

func newTestRedis(t *testing.T) *redis.Client {
	mr, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	assert.Nil(t, client.Ping(context.TODO()).Err())

	return client
}

func TestNew(t *testing.T) {
	redisClient := newTestRedis(t)

	testPrefix := "TEST:"
	testNamespace := "namespace"
	testGroup := "group"
	testEndpoint := "/test/endpoint"
	testItem := fastcache.Item{
		ETag:        "etag",
		ContentType: "content_type",
		Blob:        []byte("{}"),
	}
	for _, async := range []bool{true, false} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			pool := New(Config{
				Prefix:             testPrefix,
				Async:              async,
				AsyncMaxCommitSize: 5,
				AsyncBufSize:       10,
				AsyncCommitFreq:    100 * time.Millisecond,
			}, redisClient)

			// Check empty get, should return proper error and not panic.
			_, err := pool.Get(testNamespace, testGroup, testEndpoint)
			assert.NotNil(t, err)

			// Place something in cache,
			err = pool.Put(testNamespace, testGroup, testEndpoint, testItem, time.Second*3)
			assert.Nil(t, err)

			if async {
				time.Sleep(200 * time.Millisecond)
			}

			// Retrieve cache
			item, err := pool.Get(testNamespace, testGroup, testEndpoint)
			assert.Nil(t, err)
			assert.Equal(t, testItem, item)

			// Invalidate
			err = pool.Del(testNamespace, testGroup, testEndpoint)
			assert.Nil(t, err)

			// Check empty get, should return proper error and not panic.
			_, err = pool.Get(testNamespace, testGroup, testEndpoint)
			assert.NotNil(t, err)

			// Invalidate
			err = pool.DelGroup(testNamespace, testGroup)
			assert.Nil(t, err)

			// Check empty get, should return proper error and not panic.
			_, err = pool.Get(testNamespace, testGroup, testEndpoint)
			assert.NotNil(t, err)
		})
	}
}
