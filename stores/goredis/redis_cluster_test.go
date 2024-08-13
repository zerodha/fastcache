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

func newTestRedisCluster(t *testing.T) (*redis.ClusterClient, []*miniredis.Miniredis) {
	// Create three miniredis instances
	mr1, err := miniredis.Run()
	assert.NoError(t, err)
	mr2, err := miniredis.Run()
	assert.NoError(t, err)
	mr3, err := miniredis.Run()
	assert.NoError(t, err)

	// Create a cluster client
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{mr1.Addr(), mr2.Addr(), mr3.Addr()},
		// In a real scenario, you'd need to configure the cluster properly
		// This is a simplified setup for testing purposes
	})

	assert.NoError(t, client.Ping(context.TODO()).Err())

	return client, []*miniredis.Miniredis{mr1, mr2, mr3}
}

func TestAsyncWritesToCluster(t *testing.T) {
	redisCluster, _ := newTestRedisCluster(t)

	testPrefix := "TEST:"
	testNamespace := "namespace"
	testGroup := "group"
	testEndpoint := "/test/endpoint"
	testItem := fastcache.Item{
		ETag:        "etag",
		ContentType: "content_type",
		Blob:        []byte("{}"),
	}

	pool := New(Config{
		Prefix:             testPrefix,
		Async:              true,
		AsyncMaxCommitSize: 5,
		AsyncBufSize:       10,
		AsyncBufTimeout:    100 * time.Millisecond,
	}, redisCluster)

	// Perform async writes
	for i := 0; i < 20; i++ {
		err := pool.Put(testNamespace, testGroup, fmt.Sprintf("%s%d", testEndpoint, i), testItem, time.Second*3)
		assert.NoError(t, err)
	}

	// Wait for async writes to complete
	time.Sleep(200 * time.Millisecond)

	// Verify the writes
	for i := 0; i < 20; i++ {
		item, err := pool.Get(testNamespace, testGroup, fmt.Sprintf("%s%d", testEndpoint, i))
		assert.NoError(t, err)
		assert.Equal(t, testItem, item)
	}

	// Test deletion
	err := pool.DelGroup(testNamespace, testGroup)
	assert.NoError(t, err)

	// Verify deletion
	_, err = pool.Get(testNamespace, testGroup, fmt.Sprintf("%s%d", testEndpoint, 0))
	assert.Error(t, err)
}

// func TestAsyncWritesWithNodeFailure(t *testing.T) {
// 	redisCluster, miniredisInstances := newTestRedisCluster(t)

// 	testPrefix := "TEST:"
// 	testNamespace := "namespace"
// 	testGroup := "group"
// 	testEndpoint := "/test/endpoint"
// 	testItem := fastcache.Item{
// 		ETag:        "etag",
// 		ContentType: "content_type",
// 		Blob:        []byte("{}"),
// 	}

// 	pool := New(Config{
// 		Prefix:             testPrefix,
// 		Async:              true,
// 		AsyncMaxCommitSize: 50,
// 		AsyncBufSize:       50,
// 		AsyncBufTimeout:    100 * time.Millisecond,
// 	}, redisCluster)

// 	// Start async writes
// 	for i := 0; i < 10; i++ {
// 		err := pool.Put(testNamespace, testGroup, fmt.Sprintf("%s%d", testEndpoint, i), testItem, time.Second*3)
// 		assert.NoError(t, err)
// 	}

// 	// Simulate node failure by stopping one of the miniredis instances
// 	miniredisInstances[1].Close()

// 	// Continue with more writes
// 	for i := 10; i < 20; i++ {
// 		err := pool.Put(testNamespace, testGroup, fmt.Sprintf("%s%d", testEndpoint, i), testItem, time.Second*3)
// 		assert.NoError(t, err)
// 	}

// 	// Wait for async writes to complete
// 	time.Sleep(500 * time.Millisecond)

// 	// Verify the writes
// 	successCount := 0
// 	for i := 0; i < 20; i++ {
// 		item, err := pool.Get(testNamespace, testGroup, fmt.Sprintf("%s%d", testEndpoint, i))
// 		if err == nil &&
// 			item.ETag == testItem.ETag &&
// 			item.ContentType == testItem.ContentType &&
// 			bytes.Equal(item.Blob, testItem.Blob) &&
// 			item.Compression == testItem.Compression {
// 			successCount++
// 		}
// 	}

// 	// We expect some writes to succeed, but not all due to the node failure
// 	assert.True(t, successCount > 0 && successCount < 20, "Expected some successful writes, but not all. Got %d successful writes", successCount)

// 	// Test deletion
// 	err := pool.DelGroup(testNamespace, testGroup)
// 	assert.NoError(t, err)

// 	// Verify deletion
// 	_, err = pool.Get(testNamespace, testGroup, fmt.Sprintf("%s%d", testEndpoint, 0))
// 	assert.Error(t, err)
// }
