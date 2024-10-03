//go:build clustertest
// +build clustertest

package goredis

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zerodha/fastcache/v4"
)

const (
	redisImage = "docker.io/bitnami/redis-cluster:6.0-debian-10"
	numNodes   = 6
)

func setupRedisCluster(t *testing.T) (*redis.ClusterClient, func(), func(int)) {
	ctx := context.Background()

	// Create a network
	// Incase of a network error, try running the following command:
	// docker network rm fc-redis-cluster-network
	networkName := "fc-redis-cluster-network"
	network, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name:   networkName,
			Driver: "bridge",
		},
	})
	require.NoError(t, err)

	// Start Redis nodes
	nodes := make([]testcontainers.Container, numNodes)
	wg := sync.WaitGroup{}
	wg.Add(numNodes)
	for i := 0; i < numNodes; i++ {
		go func(i int) {
			defer wg.Done()
			nodeName := fmt.Sprintf("redis-node-%d", i)
			req := testcontainers.ContainerRequest{
				Image:        redisImage,
				ExposedPorts: []string{"6379/tcp"},
				Name:         nodeName,
				Networks:     []string{networkName},
				Env: map[string]string{
					"ALLOW_EMPTY_PASSWORD": "yes",
					"REDIS_NODES":          "redis-node-0 redis-node-1 redis-node-2 redis-node-3 redis-node-4 redis-node-5",
				},
				WaitingFor: wait.ForLog(".*(?:Background AOF rewrite finished successfully|Synchronization with replica (?:\\d{1,3}\\.){3}\\d{1,3}:\\d+ succeeded).*").AsRegexp(),
			}

			node, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
				ContainerRequest: req,
				Started:          true,
			})
			require.NoError(t, err)
			nodes[i] = node
		}(i)
	}

	// Initialize the cluster
	initReq := testcontainers.ContainerRequest{
		Image:    redisImage,
		Name:     "redis-cluster-init",
		Networks: []string{networkName},
		Env: map[string]string{
			"ALLOW_EMPTY_PASSWORD":   "yes",
			"REDIS_CLUSTER_REPLICAS": "1",
			"REDIS_NODES":            "redis-node-0 redis-node-1 redis-node-2 redis-node-3 redis-node-4 redis-node-5",
			"REDIS_CLUSTER_CREATOR":  "yes",
		},
		WaitingFor: wait.ForLog(""),
	}

	initContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: initReq,
		Started:          true,
	})
	require.NoError(t, err)

	wg.Wait()
	log.Println("Redis cluster initialized, and all nodes are up and running.")

	// Get the IP addresses of the nodes
	var nodeAddresses []string
	for _, node := range nodes {
		ip, err := node.ContainerIP(ctx)
		require.NoError(t, err)
		nodeAddresses = append(nodeAddresses, fmt.Sprintf("%s:6379", ip))
	}

	// Create a cluster client
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: nodeAddresses,
	})

	require.NoError(t, client.Ping(ctx).Err())

	cleanup := func() {
		client.Close()
		for _, node := range nodes {
			node.Terminate(ctx)
		}
		initContainer.Terminate(ctx)
		network.Remove(ctx)
	}

	kill := func(i int) {
		nodes[i].Stop(ctx, nil)
	}

	return client, cleanup, kill
}

func TestAsyncWritesToCluster(t *testing.T) {
	t.Skip()
	redisCluster, cleanup, _ := setupRedisCluster(t)
	defer cleanup()

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
		AsyncCommitFreq:    100 * time.Millisecond,
	}, redisCluster)

	// Perform async writes
	for i := 0; i < 20; i++ {
		err := pool.Put(testNamespace, testGroup, fmt.Sprintf("%s%d", testEndpoint, i), testItem, time.Second*3)
		require.NoError(t, err)
	}

	// Wait for async writes to complete
	time.Sleep(200 * time.Millisecond)

	// Verify the writes
	for i := 0; i < 20; i++ {
		item, err := pool.Get(testNamespace, testGroup, fmt.Sprintf("%s%d", testEndpoint, i))
		require.NoError(t, err)
		require.Equal(t, testItem, item)
	}

	// Test deletion
	err := pool.DelGroup(testNamespace, testGroup)
	require.NoError(t, err)

	// Verify deletion
	_, err = pool.Get(testNamespace, testGroup, fmt.Sprintf("%s%d", testEndpoint, 0))
	require.Error(t, err)
}

func TestAsyncWritesWithNodeFailure(t *testing.T) {
	redisCluster, cleanup, kill := setupRedisCluster(t)
	defer cleanup()

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
		AsyncMaxCommitSize: 50,
		AsyncBufSize:       50,
		AsyncCommitFreq:    100 * time.Millisecond,
	}, redisCluster)

	// Start async writes
	for i := 0; i < 10; i++ {
		err := pool.Put(testNamespace, testGroup, fmt.Sprintf("%s%d:{%d}", testEndpoint, i, i%3), testItem, time.Second*3)
		require.NoError(t, err)
	}

	// Lets complete the async writes
	time.Sleep(200 * time.Millisecond)

	// Simulate node failure by removing a node from the cluster
	kill(1)
	kill(4)

	time.Sleep(5000 * time.Millisecond)

	// Continue with more writes
	for i := 10; i < 20; i++ {
		err := pool.Put(testNamespace, testGroup, fmt.Sprintf("%s%d:{%d}", testEndpoint, i, i%3), testItem, time.Second*3)
		require.NoError(t, err)
	}

	// Wait for async writes to complete
	time.Sleep(200 * time.Millisecond)

	// Verify the writes
	successCount := 0
	for i := 0; i < 20; i++ {
		item, err := pool.Get(testNamespace, testGroup, fmt.Sprintf("%s%d:{%d}", testEndpoint, i, i%3))
		if err == nil && item.ETag == testItem.ETag && item.ContentType == testItem.ContentType {
			successCount++
		}
	}

	// We expect some writes to succeed, but not all due to the node failure
	require.True(t, successCount > 0 && successCount < 20, "Expected some successful writes, but not all. Got %d successful writes", successCount)

	// Test deletion
	err := pool.DelGroup(testNamespace, testGroup)
	require.NoError(t, err)

	// Verify deletion
	_, err = pool.Get(testNamespace, testGroup, fmt.Sprintf("%s%d", testEndpoint, 0))
	require.Error(t, err)
}
