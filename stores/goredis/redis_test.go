package goredis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/zerodha/fastcache/v2"
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
		ETag:        []byte("etag"),
		ContentType: []byte("content_type"),
		Blob:        []byte("{}"),
	}

	pool := New(testPrefix, redisClient)

	// Check empty get, should return proper error and not panic.
	_, err := pool.Get(testNamespace, testGroup, testEndpoint)
	assert.NotNil(t, err)

	// Place something in cache,
	err = pool.Put(testNamespace, testGroup, testEndpoint, testItem, time.Second*3)
	assert.Nil(t, err)

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
}
