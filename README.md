# fastcache

fastcache is a simple response caching package that plugs into fastglue. The
`Cached()` middleware can be wrapped around fastglue GET handlers that need to serve
cached bytes for a request.

The `ClearGroup()` handler is meant for invalidating cache, for
wrapping POST / PUT / DELETE handlers.

It supports arbitrary backend storage implementations and ships with redigo/go-redis store implementations.

## Concepts

#### `namespace (fastcache.Options.NamespaceKey)`.

All cached items are saved under a namespace. For authenticated calls, this is most commonly the user ID. So, all GET requests for a user XX1234 like orders, marketwatch, profile etc. may be namespaced under a XX1234 key in the store.

`fastcache.Options.NamespaceKey` is the name of the key that'll have the name of the namespace in a `RequestCtx.UserValue(NamespaceKey)`. This UserValue should be set by another middleware, such as the auth middleware, before the `Cached()` middleware is executed. For example, `RequestCtx.SetUerValue("user_id", "XX1234")` where `user_id` is the NamespaceKey. For handlers with params like `/orders/:user_id`, this is taken care of by the router.

### `group`

Cache for different URIs of the same type can be grouped under a single name so that the cache for a group can be deleted in one go when something changes. For instance, orders and tradebook handlers can be grouped "orders" and different marketwatch calls can be grouped under "mw".

## Middlewares

`Cached()` is the middleware for GET calls that does caching, 304 serving etc.

`ClearGroup()` is middleware handlers for POST / PUT / DELETE methods that are meant to clear cache for GET calls.

## Manual cache clearing

The `.Del()` and `.DelGroup()` can be used to manually clear cached items when handler based clearing isn't sufficient.

## Example

```go

    fc := fastcache.New(redisstore.New(pool))

    // Long cache options.
    long := &fastcache.Options{
        NamespaceKey: "user_id",
        ETag: true,
    }
    short := &fastcache.Options{
        NamespaceKey: "user_id",
        TTL: time.Second * 60,
        ETag: true,
    }

    g.GET("/margins", auth(fc.Cached(handleGetMargins, short, "margins")))
    g.GET("/margins/:segment", auth(fc.Cached(handleGetMargins, short, "margins")))

    g.GET("/orders", auth(fc.Cached(handleGetMargins, long, "orders")))
    g.GET("/orders", auth(fc.Cached(handleGetMargins, short, "trades")))

    // Clear the orders group. Multiple groups can be specified like: orders, positions ...
    g.DELETE("/orders/:order_id",auth(app.respcache.ClearGroup(handleDeleteMarketwatchItems, short, []string{"orders"})))
```
