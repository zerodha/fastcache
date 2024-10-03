package fastcache

import (
	"sync"
	"testing"
	"time"
)

type testSlowStore struct {
	getCount int
	delay    time.Duration
}

func (s *testSlowStore) Get(namespace, group, uri string) (Item, error) {
	time.Sleep(s.delay)
	s.getCount++
	return Item{}, nil
}

func (s *testSlowStore) Put(namespace, group, uri string, b Item, ttl time.Duration) error {
	return nil
}

func (s *testSlowStore) Del(namespace, group, uri string) error {
	return nil
}

func (s *testSlowStore) DelGroup(namespace string, group ...string) error {
	return nil
}

func TestSingleFlightStore(t *testing.T) {
	slowStore := &testSlowStore{delay: 100 * time.Millisecond}
	sfs := newSingleflightStore(slowStore)

	sfs.Put("namespace", "group", "uri", Item{}, 0)

	// Call Get 10 times concurrently
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			_, _ = sfs.Get("namespace", "group", "uri")
			wg.Done()
		}()
	}

	wg.Wait()

	if slowStore.getCount != 1 {
		t.Errorf("expected 1, got %d", slowStore.getCount)
	}
}
