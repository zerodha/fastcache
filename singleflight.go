package fastcache

import (
	"fmt"
	"time"

	"golang.org/x/sync/singleflight"
)

// singleFlightStore wraps a Store with singleflight functionality
type singleFlightStore struct {
	store Store
	sf    singleflight.Group
}

// newSingleflightStore creates a new SingleflightStore
func newSingleflightStore(store Store) *singleFlightStore {
	return &singleFlightStore{
		store: store,
	}
}

// Get retrieves an item from the store using singleflight
func (s *singleFlightStore) Get(namespace, group, uri string) (Item, error) {
	key := fmt.Sprintf("%s:%s:%s", namespace, group, uri)

	v, err, _ := s.sf.Do(key, func() (interface{}, error) {
		return s.store.Get(namespace, group, uri)
	})

	if err != nil {
		return Item{}, err
	}

	// Handle the case where the item doesn't exist
	if v == nil {
		return Item{}, nil
	}

	// Check the type of v is Item
	item, ok := v.(Item)
	if !ok {
		return Item{}, fmt.Errorf("unexpected type %T", v)
	}

	return item, nil
}

// Put adds an item to the underlying store
func (s *singleFlightStore) Put(namespace, group, uri string, b Item, ttl time.Duration) error {
	return s.store.Put(namespace, group, uri, b, ttl)
}

// Del removes an item from the underlying store
func (s *singleFlightStore) Del(namespace, group, uri string) error {
	return s.store.Del(namespace, group, uri)
}

// DelGroup removes a group of items from the underlying store
func (s *singleFlightStore) DelGroup(namespace string, group ...string) error {
	return s.store.DelGroup(namespace, group...)
}
