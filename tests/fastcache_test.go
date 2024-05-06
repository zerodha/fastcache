package tests

import (
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	redis "github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
	cachestore "github.com/zerodha/fastcache/stores/goredis/v9"
	"github.com/zerodha/fastcache/v4"
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

	content = []byte("this is the reasonbly long test content that may be compressed")
)

func init() {
	// Setup fastcache.
	rd, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	var (
		cfgDefault = &fastcache.Options{
			NamespaceKey: namespaceKey,
			ETag:         true,
			TTL:          time.Second * 5,
			Logger:       log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile),
			Compression: fastcache.CompressionsOptions{
				Enabled:   true,
				MinLength: 10,
			},
		}

		cfgCompressed = &fastcache.Options{
			NamespaceKey: namespaceKey,
			ETag:         true,
			TTL:          time.Second * 5,
			Logger:       log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile),
			Compression: fastcache.CompressionsOptions{
				Enabled:        true,
				MinLength:      10,
				RespectHeaders: true,
			},
		}

		noBlob = &fastcache.Options{
			NamespaceKey: namespaceKey,
			ETag:         true,
			TTL:          time.Second * 60,
			NoBlob:       true,
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
		return r.SendBytes(200, "text/plain", content)
	}, cfgDefault, group))

	srv.GET("/no-store", fc.Cached(func(r *fastglue.Request) error {
		r.RequestCtx.Response.Header.Set("Cache-Control", "no-store")
		return r.SendBytes(200, "text/plain", content)
	}, cfgDefault, group))

	srv.GET("/no-blob", fc.Cached(func(r *fastglue.Request) error {
		return r.SendBytes(200, "text/plain", content)
	}, noBlob, group))

	srv.GET("/compressed", fc.Cached(func(r *fastglue.Request) error {
		return r.SendBytes(200, "text/plain", content)
	}, cfgCompressed, group))

	srv.GET("/clear-group", fc.ClearGroup(func(r *fastglue.Request) error {
		return r.SendBytes(200, "text/plain", content)
	}, cfgDefault, group))

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

func getReq(url, etag string, gzipped bool, t *testing.T) (*http.Response, []byte) {
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

	if gzipped {
		req.Header.Set("Accept-Encoding", "gzip")
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(b)
	}

	return resp, b
}

func TestCache(t *testing.T) {
	// First request should be 200.
	r, b := getReq(srvRoot+"/cached", "", false, t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got %v", r.StatusCode)
	}
	if !bytes.Equal(b, content) {
		t.Fatalf("expected 'ok' in body but got %v", b)
	}

	// Second should be 304.
	r, b = getReq(srvRoot+"/cached", r.Header.Get("Etag"), false, t)
	if r.StatusCode != 304 {
		t.Fatalf("expected 304 but got '%v'", r.StatusCode)
	}
	if !bytes.Equal(b, []byte("")) {
		t.Fatalf("expected empty cached body but got '%v'", b)
	}

	// Wrong etag.
	r, b = getReq(srvRoot+"/cached", "wrong", false, t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got '%v'", r.StatusCode)
	}

	// Clear cache.
	r, b = getReq(srvRoot+"/clear-group", "", false, t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got %v", r.StatusCode)
	}
	r, b = getReq(srvRoot+"/cached", r.Header.Get("Etag"), false, t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got '%v'", r.StatusCode)
	}

	// Compressed blob.
	r, b = getReq(srvRoot+"/compressed", "", false, t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got '%v'", r.StatusCode)
	}
	// Uncompressed output.
	if !bytes.Equal(b, content) {
		t.Fatalf("expected test content in body but got %v", b)
	}

	// Compressed output.
	r, b = getReq(srvRoot+"/compressed", r.Header.Get("Etag"), true, t)
	if r.StatusCode != 304 {
		t.Fatalf("expected 304 but got '%v'", r.StatusCode)
	}

	r, b = getReq(srvRoot+"/compressed", "", true, t)
	if r.StatusCode != 200 {
		t.Fatalf("expected 200 but got '%v'", r.StatusCode)
	}

	decomp, err := decompressGzip(b)
	if err != nil {
		t.Fatalf("error decompressing gzip: %v", err)
	}

	if !bytes.Equal(decomp, content) {
		t.Fatalf("expected test content in body but got %v", b)
	}
}

func TestNoCache(t *testing.T) {
	// All requests should return 200.
	for n := 0; n < 3; n++ {
		r, b := getReq(srvRoot+"/no-store", "", false, t)
		if r.StatusCode != 200 {
			t.Fatalf("expected 200 but got %v", r.StatusCode)
		}
		if r.Header.Get("Etag") != "" {
			t.Fatal("there should be no etag for no-store response")
		}
		if !bytes.Equal(b, content) {
			t.Fatalf("expected 'ok' in body but got %v", b)
		}
	}
}

func TestNoBlob(t *testing.T) {
	// All requests should return 200.
	eTag := ""
	for n := 0; n < 3; n++ {
		r, _ := getReq(srvRoot+"/no-blob", eTag, false, t)
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

func decompressGzip(b []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}
