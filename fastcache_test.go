package fastcache_test

import (
	"io/ioutil"
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

	srv.GET("/clear-group", fc.ClearGroup(func(r *fastglue.Request) error {
		return r.SendBytes(200, "text/plain", []byte("ok"))
	}, ttlShort, group))

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

	b, err := ioutil.ReadAll(resp.Body)
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
	r, b = getReq(srvRoot+"/cached", "wrong", t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got '%v'", r.StatusCode)
	}

	// Clear cache.
	r, b = getReq(srvRoot+"/clear-group", "", t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got %v", r.StatusCode)
	}
	r, b = getReq(srvRoot+"/cached", r.Header.Get("Etag"), t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got '%v'", r.StatusCode)
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
