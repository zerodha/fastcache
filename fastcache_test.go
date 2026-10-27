package fastcache

import (
	"testing"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/zerodha/fastglue"
)

type ttlStore struct {
	item Item
	ttls []time.Duration
}

func (s *ttlStore) Get(namespace, group, uri string) (Item, error) {
	return s.item, nil
}

func (s *ttlStore) Put(namespace, group, uri string, item Item, ttl time.Duration) error {
	s.ttls = append(s.ttls, ttl)
	return nil
}

func (s *ttlStore) Del(namespace, group, uri string) error { return nil }

func (s *ttlStore) DelGroup(namespace string, group ...string) error { return nil }

func ttlRequest() *fastglue.Request {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("/test")
	ctx.SetUserValue("user_id", "test")
	return &fastglue.Request{RequestCtx: ctx}
}

func TestCustomTTL(t *testing.T) {
	for _, tt := range []struct {
		name   string
		ttl    time.Duration
		custom interface{}
		want   time.Duration
	}{
		{"unset", time.Minute, nil, time.Minute},
		{"shorter", time.Minute, 30 * time.Second, 30 * time.Second},
		{"longer", time.Minute, 2 * time.Minute, time.Minute},
		{"equal", time.Minute, time.Minute, time.Minute},
		{"zero", time.Minute, time.Duration(0), time.Minute},
		{"negative", time.Minute, -time.Second, time.Minute},
		{"string", time.Minute, "30s", time.Minute},
		{"integer", time.Minute, 30, time.Minute},
		{"no expiry", 0, 30 * time.Second, 30 * time.Second},
		{"negative default", -time.Second, 30 * time.Second, 30 * time.Second},
		{"no expiry without override", 0, nil, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := &ttlStore{}
			o := &Options{NamespaceKey: "user_id", TTL: tt.ttl}
			calls := 0
			h := New(store).Cached(func(r *fastglue.Request) error {
				calls++
				if calls == 1 && tt.custom != nil {
					r.RequestCtx.SetUserValue(CustomTTLKey, tt.custom)
				}
				return r.SendBytes(200, "text/plain", []byte("response"))
			}, o, "test")

			// Reuse the handler and options for a request with no override too.
			for _, want := range []time.Duration{tt.want, tt.ttl} {
				if err := h(ttlRequest()); err != nil {
					t.Fatal(err)
				}
				if len(store.ttls) != calls || store.ttls[calls-1] != want {
					t.Fatalf("request %d: got TTLs %v, want TTL %v", calls, store.ttls, want)
				}
				if o.TTL != tt.ttl {
					t.Fatalf("Options.TTL changed: got %v, want %v", o.TTL, tt.ttl)
				}
			}
		})
	}
}

func TestCustomTTLCacheHit(t *testing.T) {
	store := &ttlStore{item: Item{ContentType: "text/plain", Blob: []byte("cached")}}
	h := New(store).Cached(func(r *fastglue.Request) error {
		t.Fatal("handler called on a cache hit")
		return nil
	}, &Options{NamespaceKey: "user_id", TTL: time.Minute}, "test")
	r := ttlRequest()
	r.RequestCtx.SetUserValue(CustomTTLKey, time.Second)
	if err := h(r); err != nil {
		t.Fatal(err)
	}
	if len(store.ttls) != 0 {
		t.Fatalf("cache hit wrote TTLs: %v", store.ttls)
	}
	if got := string(r.RequestCtx.Response.Body()); got != "cached" {
		t.Fatalf("got body %q, want cached", got)
	}
}

func TestCustomTTLUncacheableResponse(t *testing.T) {
	for _, tt := range []struct {
		name    string
		status  int
		noStore bool
	}{
		{"no-store", 200, true},
		{"error", 500, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := &ttlStore{}
			h := New(store).Cached(func(r *fastglue.Request) error {
				r.RequestCtx.SetUserValue(CustomTTLKey, time.Second)
				if tt.noStore {
					r.RequestCtx.Response.Header.Set("Cache-Control", "no-store")
				}
				return r.SendBytes(tt.status, "text/plain", []byte("response"))
			}, &Options{NamespaceKey: "user_id", TTL: time.Minute}, "test")
			if err := h(ttlRequest()); err != nil {
				t.Fatal(err)
			}
			if len(store.ttls) != 0 {
				t.Fatalf("uncacheable response wrote TTLs: %v", store.ttls)
			}
		})
	}
}
