package redis

// A Handler responds to a redis request.
//
// ServeRedis should write reply values through the WriteLen and Write methods
// of the ResponseWriter res, responding to the request req.
type Handler interface {
	ServeRedis(res ResponseWriter, req *Request)
}

// HandlerFunc is an adapter to allow the use of ordinary functions as redis
// handlers.
type HandlerFunc func(ResponseWriter, *Request)

// ServeRedis calls f(res, req).
func (f HandlerFunc) ServeRedis(res ResponseWriter, req *Request) { f(res, req) }
