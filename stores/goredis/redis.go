// Package goredis implements a Redis cache storage backend for fastcache.
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
package goredis

import (
	"context"
	"errors"
	"time"
	"unsafe"

	"github.com/go-redis/redis/v8"
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
	cn     redis.UniversalClient
	ctx    context.Context
}

// New creates a new Redis instance. prefix is the prefix to apply to all
// cache keys.
func New(prefix string, client redis.UniversalClient) *Store {
	return &Store{
		prefix: prefix,
		cn:     client,
		ctx:    context.TODO(),
	}
}

// Get gets the fastcache.Item for a single cached URI.
func (s *Store) Get(namespace, group, uri string) (fastcache.Item, error) {
	var (
		out fastcache.Item
	)
	// Get content_type, etag, blob in that order.
	cmd := s.cn.HMGet(s.ctx, s.key(namespace, group), s.field(keyCtype, uri), s.field(keyEtag, uri), s.field(keyCompression, uri), s.field(keyBlob, uri))
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
		out.ContentType = ctype
	} else {
		return out, errors.New("goredis-store: invalid type received for ctype")
	}

	if etag, ok := resp[1].(string); ok {
		out.ETag = etag
	} else {
		return out, errors.New("goredis-store: invalid type received for etag")
	}

	if comp, ok := resp[2].(string); ok {
		out.Compression = comp
	} else {
		return out, errors.New("goredis-store: invalid type received for etag")
	}

	if blob, ok := resp[3].(string); ok {
		out.Blob = stringToBytes(blob)
	} else {
		return out, errors.New("goredis-store: invalid type received for blob")
	}

	return out, err
}

// Put sets a value to given session but stored only on commit
func (s *Store) Put(namespace, group, uri string, b fastcache.Item, ttl time.Duration) error {
	var (
		key = s.key(namespace, group)
		p   = s.cn.Pipeline()
	)

	if err := p.HMSet(s.ctx, key, map[string]interface{}{
		s.field(keyCtype, uri):       b.ContentType,
		s.field(keyEtag, uri):        b.ETag,
		s.field(keyCompression, uri): b.Compression,
		s.field(keyBlob, uri):        b.Blob,
	}).Err(); err != nil {
		return err
	}

	// Set a TTL for the group. If one uri in cache group sets a TTL
	// then entire group will be evicted. This is a short coming of using
	// hashmap as a group. Needs some work here.
	if ttl.Seconds() > 0 {
		if err := p.PExpire(s.ctx, key, ttl).Err(); err != nil {
			return err
		}
	}

	_, err := p.Exec(s.ctx)
	return err
}

// Del deletes a single cached URI.
func (s *Store) Del(namespace, group, uri string) error {
	return s.cn.HDel(s.ctx, s.key(namespace, group),
		s.field(keyCtype, uri),
		s.field(keyEtag, uri),
		s.field(keyCompression, uri),
		s.field(keyBlob, uri)).Err()
}

// DelGroup deletes a whole group.
func (s *Store) DelGroup(namespace string, groups ...string) error {
	p := s.cn.Pipeline()
	for _, group := range groups {
		if err := p.Del(s.ctx, s.key(namespace, group)).Err(); err != nil {
			return err
		}
	}

	_, err := p.Exec(s.ctx)
	return err
}

func (s *Store) key(namespace, group string) string {
	return s.prefix + namespace + sep + group
}

func (s *Store) field(key string, uri string) string {
	return key + "_" + uri
}

// stringToBytes converts string to byte slice using unsafe.
// Copied from: https://github.com/go-redis/redis/blob/803592d454c49277405303fa6261dc090db542d2/internal/util/unsafe.go
// Context: https://github.com/redis/go-redis/issues/1618
func stringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}
