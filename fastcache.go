// Package fastcache provides a simple HTTP response caching layer that can
// be plugged into fastglue.
package fastcache

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/zerodha/fastglue"
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

	// By default, handler response bodies are cached and served. If this is
	// enabled, only ETags are cached and for response bodies, the original
	// handler is invoked.
	NoBlob bool

	// Logger is the optional logger to which errors will be written.
	Logger *log.Logger

	// Cache based on uri+querystring.
	IncludeQueryString bool

	// If IncludeQueryString is true, and we only want to use specific query params
	// to cache, then set this to the list of query params to use.
	// If this is not set, then all query params are used.
	IncludedQueryParams map[string]struct{}
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

var cacheNoStore = []byte("no-store")

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
func (f *FastCache) Cached(h fastglue.FastRequestHandler, o *Options, group string) fastglue.FastRequestHandler {
	return func(r *fastglue.Request) error {
		namespace, _ := r.RequestCtx.UserValue(o.NamespaceKey).(string)
		if namespace == "" {
			if o.Logger != nil {
				o.Logger.Printf("no namespace found in UserValue() for key '%s'", o.NamespaceKey)
			}
			return h(r)
		}

		uri := f.makeURI(r, o)

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
			if _, err := r.RequestCtx.Write(blob.Blob); err != nil && o.Logger != nil {
				o.Logger.Printf("error writing request: %v", err)
			}

			return nil
		}

		// Execute the actual handler.
		if err := h(r); err != nil {
			o.Logger.Printf("error running middleware: %v", err)
		}

		// Read the response body written by the handler and cache it.
		if r.RequestCtx.Response.StatusCode() == 200 {
			// If "no-store" is set in the cache control header, don't cache.
			if !bytes.Contains(r.RequestCtx.Response.Header.Peek("Cache-Control"), cacheNoStore) {
				if err := f.cache(r, namespace, group, o); err != nil {
					o.Logger.Println(err.Error())
				}
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
		if err := h(r); err != nil && o.Logger != nil {
			o.Logger.Printf("error running middleware: %v", err)
		}

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

func (f *FastCache) makeURI(r *fastglue.Request, o *Options) string {
	var hash [16]byte

	// If IncludeQueryString option is set then cache based on uri + md5(query_string)
	if o.IncludeQueryString {
		id := r.RequestCtx.URI().FullURI()

		// Check if we need to include only specific query params.
		if o.IncludedQueryParams != nil {
			// Acquire a copy so as to not modify the request.
			uriRaw := fasthttp.AcquireURI()
			r.RequestCtx.URI().CopyTo(uriRaw)

			q := uriRaw.QueryArgs()

			// Copy the keys to delete, and delete them later. This is to
			// avoid borking the VisitAll() iterator.
			delKeys := [][]byte{}
			q.VisitAll(func(k, v []byte) {
				if _, ok := o.IncludedQueryParams[string(k)]; !ok {
					delKeys = append(delKeys, k)
				}
			})

			// Delete the keys.
			for _, k := range delKeys {
				q.DelBytes(k)
			}

			// Get the new URI.
			id = uriRaw.FullURI()

			// Release the borrowed URI.
			fasthttp.ReleaseURI(uriRaw)
		}

		hash = md5.Sum(id)
	} else {
		hash = md5.Sum(r.RequestCtx.URI().Path())
	}

	return hex.EncodeToString(hash[:])
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
	uri := f.makeURI(r, o)

	var blob []byte
	if !o.NoBlob {
		blob = r.RequestCtx.Response.Body()
	}

	err := f.s.Put(namespace, group, uri, Item{
		ETag:        etag,
		ContentType: r.RequestCtx.Response.Header.ContentType(),
		Blob:        blob,
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
