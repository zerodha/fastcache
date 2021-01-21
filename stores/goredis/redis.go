// Package goredis implements a Redis cache storage backend for fastcache.
// The internal structure looks like this where
// XX1234 = namespace, marketwach = group
// ```
// CACHE:XX1234:marketwatch {
//     "/user/marketwatch_ctype" -> []byte
//     "/user/marketwatch_etag" -> []byte
//     "/user/marketwatch_blob" -> []byte
//     "/user/marketwatch/123_ctype" -> []byte
//     "/user/marketwatch/123_etag" -> []byte
//     "/user/marketwatch/123_blob" -> []byte
// }
// ```
package goredis

import (
	"errors"
	"time"

	"github.com/go-redis/redis"
	"REDACTED/commons/fastcache/v2"
)

const (
	// Store keys.
	keyEtag  = "_etag"
	keyCtype = "_ctype"
	keyBlob  = "_blob"

	sep = ":"
)

// Store is a Redis cache store implementation for fastcache.
type Store struct {
	prefix string
	cn     *redis.Client
}

// New creates a new Redis instance. prefix is the prefix to apply to all
// cache keys.
func New(prefix string, client *redis.Client) *Store {
	return &Store{
		prefix: prefix,
		cn:     client,
	}
}

// Get gets the fastcache.Item for a single cached URI.
func (s *Store) Get(namespace, group, uri string) (fastcache.Item, error) {
	var (
		out fastcache.Item
	)
	// Get content_type, etag, blob in that order.
	cmd := s.cn.HMGet(s.key(namespace, group), s.field(keyCtype, uri), s.field(keyEtag, uri), s.field(keyBlob, uri))
	if err := cmd.Err(); err != nil {
		return out, err
	}

	resp, err := cmd.Result()
	if err != nil {
		return out, err
	}

	if resp[0] == nil || resp[1] == nil || resp[2] == nil {
		return out, errors.New("goredis-store: nil received")
	}

	if ctype, ok := resp[0].(string); ok {
		out.ContentType = []byte(ctype)
	} else {
		return out, errors.New("goredis-store: invalid type received for ctype")
	}

	if etag, ok := resp[1].(string); ok {
		out.ETag = []byte(etag)
	} else {
		return out, errors.New("goredis-store: invalid type received for etag")
	}

	if blob, ok := resp[2].(string); ok {
		out.Blob = []byte(blob)
	} else {
		return out, errors.New("goredis-store: invalid type received for blob")
	}

	return out, err
}

// Put sets a value to given session but stored only on commit
func (s *Store) Put(namespace, group, uri string, b fastcache.Item, ttl time.Duration) error {
	key := s.key(namespace, group)
	cmd := s.cn.HMSet(key,
		map[string]interface{}{
			s.field(keyCtype, uri): b.ContentType,
			s.field(keyEtag, uri):  b.ETag,
			s.field(keyBlob, uri):  b.Blob,
		})
	if err := cmd.Err(); err != nil {
		return err
	}

	// Set a TTL for the group. If one uri in cache group sets a TTL
	// then entire group will be evicted. This is a short coming of using
	// hashmap as a group. Needs some work here.
	if ttl.Seconds() > 0 {
		cmd := s.cn.PExpire(key, ttl)
		if err := cmd.Err(); err != nil {
			return err
		}
	}

	return nil
}

// Del deletes a single cached URI.
func (s *Store) Del(namespace, group, uri string) error {
	cmd := s.cn.HDel(s.key(namespace, group), s.field(keyCtype, uri), s.field(keyEtag, uri), s.field(keyBlob, uri))
	if err := cmd.Err(); err != nil {
		return err
	}

	return nil
}

// DelGroup deletes a whole group.
func (s *Store) DelGroup(namespace string, groups ...string) error {
	for _, group := range groups {
		cmd := s.cn.Del(s.key(namespace, group))
		if err := cmd.Err(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) key(namespace, group string) string {
	return s.prefix + namespace + sep + group
}

func (s *Store) field(key string, uri string) string {
	return key + "_" + uri
}
