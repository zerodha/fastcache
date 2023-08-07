package fastcache_test

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	redis "github.com/go-redis/redis/v8"
	"github.com/valyala/fasthttp"
	"github.com/zerodha/fastcache/v3"
	cachestore "github.com/zerodha/fastcache/v3/stores/goredis"
	"github.com/zerodha/fastglue"
)

const (
	srvAddr      = ":10200"
	srvRoot      = "http://127.0.0.1:10200"
	namespaceKey = "req"
	group        = "test"
)

var (
	srv = fastglue.NewGlue()
)

func init() {
	// Setup fastcache.
	rd, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	var (
		ttlShort = &fastcache.Options{
			NamespaceKey: namespaceKey,
			ETag:         true,
			TTL:          time.Second * 5,
			Logger:       log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile),
		}

		noBlob = &fastcache.Options{
			NamespaceKey: namespaceKey,
			ETag:         true,
			TTL:          time.Second * 60,
			NoBlob:       true,
			Logger:       log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile),
		}

		includeQS = &fastcache.Options{
			NamespaceKey:       namespaceKey,
			ETag:               true,
			TTL:                time.Second * 5,
			Logger:             log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile),
			IncludeQueryString: true,
		}

		includeQSNoEtag = &fastcache.Options{
			NamespaceKey:       namespaceKey,
			ETag:               false,
			TTL:                time.Second * 5,
			Logger:             log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile),
			IncludeQueryString: true,
		}

		includeQSSpecific = &fastcache.Options{
			NamespaceKey:       namespaceKey,
			ETag:               true,
			TTL:                time.Second * 5,
			Logger:             log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile),
			IncludeQueryString: true,
			QueryArgsTransformerHook: func(args *fasthttp.Args) {
				// Copy the keys to delete, and delete them later. This is to
				// avoid borking the VisitAll() iterator.
				mp := map[string]struct{}{
					"foo": {},
				}

				delKeys := [][]byte{}
				args.VisitAll(func(k, v []byte) {
					if _, ok := mp[string(k)]; !ok {
						delKeys = append(delKeys, k)
					}
				})

				// Delete the keys.
				for _, k := range delKeys {
					args.DelBytes(k)
				}
			},
		}

		fc = fastcache.New(cachestore.New("CACHE:", redis.NewClient(&redis.Options{
			Addr: rd.Addr(),
		})))
	)

	// Handlers.
	srv.Before(func(r *fastglue.Request) *fastglue.Request {
		r.RequestCtx.SetUserValue(namespaceKey, "test")
		return r
	})

	srv.GET("/cached", fc.Cached(func(r *fastglue.Request) error {
		return r.SendBytes(200, "text/plain", []byte("ok"))
	}, ttlShort, group))

	srv.GET("/no-store", fc.Cached(func(r *fastglue.Request) error {
		r.RequestCtx.Response.Header.Set("Cache-Control", "no-store")
		return r.SendBytes(200, "text/plain", []byte("ok"))
	}, ttlShort, group))

	srv.GET("/no-blob", fc.Cached(func(r *fastglue.Request) error {
		return r.SendBytes(200, "text/plain", []byte("ok"))
	}, noBlob, group))

	srv.GET("/clear-group", fc.ClearGroup(func(r *fastglue.Request) error {
		return r.SendBytes(200, "text/plain", []byte("ok"))
	}, ttlShort, group))

	srv.GET("/include-qs", fc.Cached(func(r *fastglue.Request) error {
		return r.SendBytes(200, "text/plain", []byte("ok"))
	}, includeQS, group))

	srv.GET("/include-qs-no-etag", fc.Cached(func(r *fastglue.Request) error {
		out := time.Now()
		return r.SendBytes(200, "text/plain", []byte(fmt.Sprintf("%v", out)))
	}, includeQSNoEtag, group))

	srv.GET("/include-qs-specific", fc.Cached(func(r *fastglue.Request) error {
		return r.SendBytes(200, "text/plain", []byte("ok"))
	}, includeQSSpecific, group))

	// Start the server
	go func() {
		s := &fasthttp.Server{
			Name:         "test",
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 5 * time.Second,
		}
		if err := srv.ListenAndServe(srvAddr, "", s); err != nil {
			log.Fatalf("error starting HTTP server: %s", err)
		}
	}()

	time.Sleep(time.Millisecond * 100)
}

func getReq(url, etag string, t *testing.T) (*http.Response, string) {
	client := http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}

	if etag != "" {
		req.Header = http.Header{
			"If-None-Match": []string{etag},
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(b)
	}

	return resp, string(b)
}

func TestCache(t *testing.T) {
	// First request should be 200.
	r, b := getReq(srvRoot+"/cached", "", t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got %v", r.StatusCode)
	}
	if b != "ok" {
		t.Fatalf("expected 'ok' in body but got %v", b)
	}

	// Second should be 304.
	r, b = getReq(srvRoot+"/cached", r.Header.Get("Etag"), t)
	if r.StatusCode != 304 {
		t.Fatalf("expected 304 but got '%v'", r.StatusCode)
	}
	if b != "" {
		t.Fatalf("expected empty cached body but got '%v'", b)
	}

	// Wrong etag.
	r, _ = getReq(srvRoot+"/cached", "wrong", t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got '%v'", r.StatusCode)
	}

	// Clear cache.
	r, _ = getReq(srvRoot+"/clear-group", "", t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got %v", r.StatusCode)
	}
	r, _ = getReq(srvRoot+"/cached", r.Header.Get("Etag"), t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got '%v'", r.StatusCode)
	}
}

func TestQueryString(t *testing.T) {
	// First request should be 200.
	r, b := getReq(srvRoot+"/include-qs?foo=bar", "", t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got %v", r.StatusCode)
	}

	if b != "ok" {
		t.Fatalf("expected 'ok' in body but got %v", b)
	}

	// Second should be 304.
	r, _ = getReq(srvRoot+"/include-qs?foo=bar", r.Header.Get("Etag"), t)
	if r.StatusCode != 304 {
		t.Fatalf("expected 304 but got '%v'", r.StatusCode)
	}
}

func TestQueryStringLexicographical(t *testing.T) {
	// First request should be 200.
	r, b := getReq(srvRoot+"/include-qs?foo=bar&baz=qux", "", t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got %v", r.StatusCode)
	}

	if b != "ok" {
		t.Fatalf("expected 'ok' in body but got %v", b)
	}

	// Second should be 304.
	r, _ = getReq(srvRoot+"/include-qs?baz=qux&foo=bar", r.Header.Get("Etag"), t)
	if r.StatusCode != 304 {
		t.Fatalf("expected 304 but got '%v'", r.StatusCode)
	}
}

func TestQueryStringWithoutEtag(t *testing.T) {
	// First request should be 200.
	r, b := getReq(srvRoot+"/include-qs-no-etag?foo=bar", "", t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got %v", r.StatusCode)
	}

	// Second should be 200 but with same response.
	r2, b2 := getReq(srvRoot+"/include-qs-no-etag?foo=bar", "", t)
	if r2.StatusCode != 200 {
		t.Fatalf("expected 200 but got '%v'", r2.StatusCode)
	}

	if b2 != b {
		t.Fatalf("expected '%v' in body but got %v", b, b2)
	}

	// Third should be 200 but with different response.
	r3, b3 := getReq(srvRoot+"/include-qs-no-etag?foo=baz", "", t)
	if r3.StatusCode != 200 {
		t.Fatalf("expected 200 but got '%v'", r3.StatusCode)
	}

	// time should be different
	if b3 == b {
		t.Fatalf("expected both to be different (should not be %v), but got %v", b, b3)
	}
}

func TestQueryStringSpecific(t *testing.T) {
	// First request should be 200.
	r1, b := getReq(srvRoot+"/include-qs-specific?foo=bar&baz=qux", "", t)
	if r1.StatusCode != 200 {
		t.Fatalf("expected 200 but got %v", r1.StatusCode)
	}
	if b != "ok" {
		t.Fatalf("expected 'ok' in body but got %v", b)
	}

	// Second should be 304.
	r, _ := getReq(srvRoot+"/include-qs-specific?foo=bar&baz=qux", r1.Header.Get("Etag"), t)
	if r.StatusCode != 304 {
		t.Fatalf("expected 304 but got '%v'", r.StatusCode)
	}

	// Third should be 304 as foo=bar
	r, _ = getReq(srvRoot+"/include-qs-specific?loo=mar&foo=bar&baz=qux&quux=quuz", r1.Header.Get("Etag"), t)
	if r.StatusCode != 304 {
		t.Fatalf("expected 304 but got '%v'", r.StatusCode)
	}

	// Fourth should be 200 as foo=rab
	r, b = getReq(srvRoot+"/include-qs-specific?foo=rab&baz=qux&quux=quuz", r1.Header.Get("Etag"), t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got '%v'", r.StatusCode)
	}
	if b != "ok" {
		t.Fatalf("expected 'ok' in body but got %v", b)
	}
}

func TestNoCache(t *testing.T) {
	// All requests should return 200.
	for n := 0; n < 3; n++ {
		r, b := getReq(srvRoot+"/no-store", "", t)
		if r.StatusCode != 200 {
			t.Fatalf("expected 200 but got %v", r.StatusCode)
		}
		if r.Header.Get("Etag") != "" {
			t.Fatal("there should be no etag for no-store response")
		}
		if b != "ok" {
			t.Fatalf("expected 'ok' in body but got %v", b)
		}
	}
}

func TestNoBlob(t *testing.T) {
	// All requests should return 200.
	eTag := ""
	for n := 0; n < 3; n++ {
		r, _ := getReq(srvRoot+"/no-blob", eTag, t)
		if n == 0 {
			eTag = r.Header.Get("Etag")
			if r.StatusCode != 200 {
				t.Fatalf("expected 200 but got %v", r.StatusCode)
			}
			continue
		}

		if r.StatusCode != 304 {
			t.Fatalf("expected 304 but got %v", r.StatusCode)
		}
	}
}
