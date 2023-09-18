// Package redis implements a Redis cache storage backend for fastcache.
// The internal structure looks like this where
// XX1234 = namespace, marketwach = group
// ```
//
//	CACHE:XX1234:marketwatch {
//	    "/user/marketwatch_ctype" -> []byte
//	    "/user/marketwatch_etag" -> []byte
//	    "/user/marketwatch_blob" -> []byte
//	    "/user/marketwatch/123_ctype" -> []byte
//	    "/user/marketwatch/123_etag" -> []byte
//	    "/user/marketwatch/123_blob" -> []byte
//	}
//
// ```
package redis

import (
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/zerodha/fastcache/v3"
)

const (
	// Store keys.
	keyEtag        = "_etag"
	keyCtype       = "_ctype"
	keyCompression = "_comp"
	keyBlob        = "_blob"

	sep = ":"
)

// Store is a Redis cache store implementation for fastcache.
type Store struct {
	prefix string
	pool   *redis.Pool
}

// New creates a new Redis instance. prefix is the prefix to apply to all
// cache keys.
func New(prefix string, pool *redis.Pool) *Store {
	return &Store{
		prefix: prefix,
		pool:   pool,
	}
}

// Get gets the fastcache.Item for a single cached URI.
func (s *Store) Get(namespace, group, uri string) (fastcache.Item, error) {
	cn := s.pool.Get()
	defer cn.Close()

	var out fastcache.Item
	// Get content_type, etag, blob in that order.
	resp, err := redis.ByteSlices(cn.Do("HMGET", s.key(namespace, group), s.field(keyCtype, uri), s.field(keyEtag, uri), s.field(keyCompression, uri), s.field(keyBlob, uri)))
	if err != nil {
		return out, err
	}

	out = fastcache.Item{
		ContentType: string(resp[0]),
		ETag:        string(resp[1]),
		Compression: string(resp[2]),
		Blob:        resp[3],
	}
	return out, err
}

// Put sets a value to given session but stored only on commit
func (s *Store) Put(namespace, group, uri string, b fastcache.Item, ttl time.Duration) error {
	cn := s.pool.Get()
	defer cn.Close()

	key := s.key(namespace, group)
	if err := cn.Send("HMSET", key,
		s.field(keyCtype, uri), b.ContentType,
		s.field(keyEtag, uri), b.ETag,
		s.field(keyCompression, uri), b.Compression,
		s.field(keyBlob, uri), b.Blob); err != nil {
		return err
	}

	// Set a TTL for the group. If one uri in cache group sets a TTL
	// then entire group will be evicted. This is a short coming of using
	// hashmap as a group. Needs some work here.
	if ttl.Seconds() > 0 {
		exp := ttl.Nanoseconds() / int64(time.Millisecond)
		if err := cn.Send("PEXPIRE", key, exp); err != nil {
			return err
		}
	}
	return cn.Flush()
}

// Del deletes a single cached URI.
func (s *Store) Del(namespace, group, uri string) error {
	cn := s.pool.Get()
	defer cn.Close()

	if err := cn.Send("HDEL", s.key(namespace, group), s.field(keyCtype, uri), s.field(keyEtag, uri), s.field(keyCompression, uri), s.field(keyBlob, uri)); err != nil {
		return err
	}

	return cn.Flush()
}

// DelGroup deletes a whole group.
func (s *Store) DelGroup(namespace string, groups ...string) error {
	cn := s.pool.Get()
	defer cn.Close()

	for _, group := range groups {
		if err := cn.Send("DEL", s.key(namespace, group)); err != nil {
			return err
		}
	}
	return cn.Flush()
}

func (s *Store) key(namespace, group string) string {
	return s.prefix + namespace + sep + group
}

func (s *Store) field(key string, uri string) string {
	return key + "_" + uri
}
