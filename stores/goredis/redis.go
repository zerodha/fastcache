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
//
// This library also supports async mode which is dependent on the go-redis
// library. ref:
// https://github.com/redis/go-redis/discussions/2597#discussioncomment-5909650
//
// ```
package goredis

import (
	"context"
	"errors"
	"io"
	"log"
	"time"
	"unsafe"

	"github.com/redis/go-redis/v9"
	"github.com/zerodha/fastcache/v4"
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
	config Config
	putBuf chan putReq
	cn     redis.UniversalClient
	ctx    context.Context
	logger *log.Logger
}

type Config struct {
	// Prefix is the prefix to apply to all cache keys.
	// Note: in async mode you can use braces to specify the {sharding_key}.
	Prefix string

	// Async enables async writes to Redis. If enabled, writes are buffered
	// and committed in batches.
	Async bool
	// AsyncMaxCommitSize is the maximum number of writes to commit in a single
	// batch.
	AsyncMaxCommitSize int
	// AsyncBufSize is the size of the write buffer, i.e. the channel size for
	// async writes. If the buffer is full, writes will block; so make sure to
	// set this to a reasonable value, ideally higher than maxCommitSize.
	AsyncBufSize int
	// AsyncCommitFreq is the time to wait before committing the write
	// buffer.
	AsyncCommitFreq time.Duration
	// Logger is an optional logger to which errors will be written. If it is
	// nil, errors are sent to io.Discard.
	Logger *log.Logger
}

// New creates a new Redis instance. prefix is the prefix to apply to all
// cache keys.
func New(cfg Config, client redis.UniversalClient) *Store {
	s := &Store{
		config: cfg,
		cn:     client,
		logger: cfg.Logger,
		ctx:    context.TODO(),
	}

	if s.logger == nil {
		s.logger = log.New(io.Discard, "", 0)
	}

	// Start the async worker if enabled.
	if cfg.Async {
		// Set defaults.
		if s.config.AsyncBufSize == 0 {
			s.config.AsyncBufSize = 1000
		}

		if s.config.AsyncMaxCommitSize == 0 {
			s.config.AsyncMaxCommitSize = 100
		}

		if s.config.AsyncCommitFreq == 0 {
			s.config.AsyncCommitFreq = 100 * time.Millisecond
		}

		s.putBuf = make(chan putReq, s.config.AsyncBufSize)
		go s.putWorker()
	}

	return s
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

type putReq struct {
	namespace string
	group     string
	uri       string
	b         fastcache.Item
	ttl       time.Duration
}

// Put sets a value to given session but stored only on commit
func (s *Store) Put(namespace, group, uri string, b fastcache.Item, ttl time.Duration) error {
	if s.config.Async {
		// In async mode, we need to copy b.Blob to prevent fasthttp from reusing
		// the buffer, as we will use the buffer in a separate goroutine beyond
		// the scope of the current request.
		blobCopy := make([]byte, len(b.Blob))
		copy(blobCopy, b.Blob)
		b.Blob = blobCopy

		// Send the put request to the async buffer channel.
		s.putBuf <- putReq{namespace, group, uri, b, ttl}
		return nil
	}

	return s.putSync(namespace, group, uri, b, ttl)
}

func (s *Store) putSync(namespace, group, uri string, b fastcache.Item, ttl time.Duration) error {
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

func (s *Store) putWorker() {
	var (
		p      = s.cn.Pipeline()
		count  = 0
		ticker = time.NewTicker(s.config.AsyncCommitFreq)
	)
	defer ticker.Stop()

	for {
		select {
		case req := <-s.putBuf:
			key := s.key(req.namespace, req.group)
			if err := p.HMSet(s.ctx, key, map[string]interface{}{
				s.field(keyCtype, req.uri):       req.b.ContentType,
				s.field(keyEtag, req.uri):        req.b.ETag,
				s.field(keyCompression, req.uri): req.b.Compression,
				s.field(keyBlob, req.uri):        req.b.Blob,
			}).Err(); err != nil {
				// Log error
				continue
			}

			// Set a TTL for the group. If one uri in cache group sets a TTL
			// then entire group will be evicted. This is a shortcoming of using
			// hashmap as a group. Needs some work here.
			if req.ttl.Seconds() > 0 {
				if err := p.PExpire(s.ctx, key, req.ttl).Err(); err != nil {
					// Log error
					continue
				}
			}

			if count++; count > s.config.AsyncMaxCommitSize {
				if _, err := p.Exec(s.ctx); err != nil {
					s.logger.Printf("goredis-store: error committing async writes: %v", err)
				}
				count = 0
				p = s.cn.Pipeline()
			}

		case <-ticker.C:
			if count > 0 {
				if _, err := p.Exec(s.ctx); err != nil {
					s.logger.Printf("goredis-store: error committing ticker async writes: %v", err)
				}
				count = 0
				p = s.cn.Pipeline()
			}

		case <-s.ctx.Done():
			return
		}
	}
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
	if len(groups) == 0 {
		return nil
	}

	keys := make([]string, len(groups))
	for i, group := range groups {
		keys[i] = s.key(namespace, group)
	}

	return s.cn.Del(s.ctx, keys...).Err()
}

func (s *Store) key(namespace, group string) string {
	return s.config.Prefix + namespace + sep + group
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
