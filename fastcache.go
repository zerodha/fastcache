// Package fastcache provides a simple HTTP response caching layer that can
// be plugged into fastglue.
package fastcache

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"log"
	"time"

	"github.com/valyala/fasthttp"
	"REDACTED/fastglue"
)

// FastCache is the cache controller.
type FastCache struct {
	s Store
}

// Options has FastCache options.
type Options struct {
	// namespaceKey is the namespace that is used to namespace and store cache values.
	// The value of the key is obtained from RequestCtx.UserValue(namespaceKey).
	// This should be set by a middleware (such as auth) before the cache
	// middleware is called. For authenticated calls, this is most commonly
	// be a user id so that all URIs for a particular user are cached under
	// the user's namespace.
	NamespaceKey string

	// TTL for a cache item. If this is not set, no TTL is applied to cached
	// items.
	TTL time.Duration

	// Process ETags and send 304s?
	ETag bool

	// Logger is the optional logger to which errors will be written.
	Logger *log.Logger
}

// Item represents the cache entry for a single endpoint with the actual cache
// body and metadata.
type Item struct {
	ContentType []byte
	ETag        []byte
	Blob        []byte
}

// Store represents a backend data store where bytes are cached. Individual
// keys are namespaced under
type Store interface {
	Get(namespace, group, uri string) (Item, error)
	Put(namespace, group, uri string, b Item, ttl time.Duration) error
	Del(namespace, group, uri string) error
	DelGroup(namespace string, group ...string) error
}

// New creates and returns a new FastCache instance.
func New(s Store) *FastCache {
	return &FastCache{
		s: s,
	}
}

// Cached middleware "dumb" caches 200 HTTP responses as bytes with an optional TTL.
// This is used to wrap GET calls that need response cache.
//
// In addition to retrieving / caching HTTP responses, it also accepts
// ETags from clients and sends a 304 response with no actual body
// in case there's an ETag match.
//
// group is the name for the group of requests. For instance, all the GET
// requests for orders can have the group "orders" so that they can be cleared
// in one shot when something changes using the Del*() methods or Clear*() middleware.
func (f *FastCache) Cached(h fastglue.FastRequestHandler, group string, o *Options) fastglue.FastRequestHandler {
	return func(r *fastglue.Request) error {
		namespace, _ := r.RequestCtx.UserValue(o.NamespaceKey).(string)
		if namespace == "" {
			if o.Logger != nil {
				o.Logger.Printf("no namespace found in UserValue() for key '%s'", o.NamespaceKey)
			}
			return h(r)
		}
		uri := string(r.RequestCtx.Path())

		// Fetch etag + cached bytes from the store.
		blob, err := f.s.Get(namespace, group, uri)
		if err != nil && o.Logger != nil {
			o.Logger.Printf("error reading cache: %v", err)
		}

		// If ETag matching is enabled, attempt to match the header etag
		// with the stored one (if there's any).
		if o.ETag {
			var (
				match = r.RequestCtx.Request.Header.Peek("If-None-Match")
			)
			if len(match) > 4 && len(blob.ETag) > 0 && bytes.Contains(match, blob.ETag) {
				r.RequestCtx.SetStatusCode(fasthttp.StatusNotModified)
				return nil
			}
		}

		// There's cache. Write it and end the request.
		if len(blob.Blob) > 0 {
			if o.ETag {
				r.RequestCtx.Response.Header.Add("ETag", `"`+string(blob.ETag)+`"`)
			}
			r.RequestCtx.SetStatusCode(fasthttp.StatusOK)
			r.RequestCtx.SetContentTypeBytes(blob.ContentType)
			r.RequestCtx.Write(blob.Blob)
			return nil
		}

		// Execute the actual handler.
		h(r)

		// Read the response body written by the handler and cache it.
		if r.RequestCtx.Response.StatusCode() == 200 {
			if err := f.cache(r, namespace, group, o); err != nil {
				o.Logger.Println(err.Error())
			}
		}
		return nil
	}
}

// ClearGroup middleware clears cache set by the Cached() middleware
// for the all the specified groups.
//
// This should ideally wrap write handlers (POST / PUT / DELETE)
// and the cache is cleared when the handler responds with a 200.
func (f *FastCache) ClearGroup(h fastglue.FastRequestHandler, o *Options, groups ...string) fastglue.FastRequestHandler {
	return func(r *fastglue.Request) error {
		namespace, _ := r.RequestCtx.UserValue(o.NamespaceKey).(string)
		if namespace == "" {
			if o.Logger != nil {
				o.Logger.Printf("no namespace found in UserValue() for key '%s'", o.NamespaceKey)
			}
			return h(r)
		}

		// Execute the actual handler.
		h(r)

		// Clear cache.
		if r.RequestCtx.Response.StatusCode() == 200 {
			if err := f.DelGroup(namespace, groups...); err != nil && o.Logger != nil {
				o.Logger.Printf("error while deleting groups '%v': %v", groups, err)
			}
		}
		return nil
	}
}

// Del deletes the cache for a single URI in a namespace->group.
func (f *FastCache) Del(namespace, group, uri string) error {
	return f.s.Del(namespace, group, uri)
}

// DelGroup deletes all cached URIs under a group.
func (f *FastCache) DelGroup(namespace string, group ...string) error {
	return f.s.DelGroup(namespace, group...)
}

// cache caches a response body.
func (f *FastCache) cache(r *fastglue.Request, namespace, group string, o *Options) error {
	// ETag?.
	var etag []byte
	if o.ETag {
		e, err := generateRandomString(16)
		if err != nil {
			return fmt.Errorf("error generating etag: %v", err)
		}
		etag = e
	}

	// Write cache to the store (etag, content type, response body).
	uri := string(r.RequestCtx.Path())
	err := f.s.Put(namespace, group, uri, Item{
		ETag:        etag,
		ContentType: r.RequestCtx.Response.Header.ContentType(),
		Blob:        r.RequestCtx.Response.Body(),
	}, o.TTL)
	if err != nil && o.Logger != nil {
		return fmt.Errorf("error writing cache to store: %v", err)
	}

	// Send the eTag with the response.
	if o.ETag {
		r.RequestCtx.Response.Header.Add("ETag", `"`+string(etag)+`"`)
	}
	return nil
}

// generateRandomString generates a cryptographically random,
// alphanumeric string of length n.
func generateRandomString(totalLen int) ([]byte, error) {
	const dictionary = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var (
		bytes = make([]byte, totalLen)
	)
	if _, err := rand.Read(bytes); err != nil {
		return nil, err
	}

	for k, v := range bytes {
		bytes[k] = dictionary[v%byte(len(dictionary))]
	}
	return bytes, nil
}
