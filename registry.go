package redis

import "context"

// The ServerRegistry interface is an abstraction used to expose a (potentially
// changing) list of backend redis servers.
type ServerRegistry interface {
	// LookupServers returns a list of redis server endpoints.
	LookupServers(ctx context.Context) ([]ServerEndpoint, error)
}

// ServerDenyList is implemented by some ServerRegistry to support denying
// some server addresses.
type ServerDenylist interface {
	// DenyServer temporarily denies the given server endpoint.
	DenyServer(ServerEndpoint)
}

// A ServerEndpoint represents a single backend redis server.
type ServerEndpoint struct {
	Name string
	Addr string
}

// LookupServers satisfies the ServerRegistry interface.
func (endpoint ServerEndpoint) LookupServers(ctx context.Context) ([]ServerEndpoint, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return []ServerEndpoint{endpoint}, nil
	}
}

// A ServerList represents a list of backend redis servers.
type ServerList []ServerEndpoint

// LookupServers satisfies the ServerRegistry interface.
func (list ServerList) LookupServers(ctx context.Context) ([]ServerEndpoint, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		res := make([]ServerEndpoint, len(list))
		copy(res, list)
		return res, nil
	}
}
