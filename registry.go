package redis

import "context"

// The ServerRegistry interface is an abstraction used to expose a (potentially
// changing) list of backend redis servers.
type ServerRegistry interface {
	// ListServers returns a list of redis server endpoints.
	ListServers(ctx context.Context) ([]ServerEndpoint, error)
}

// A ServerEndpoint represents a single backend redis server.
type ServerEndpoint struct {
	Name string
	Addr string
}

// ListServers satisfies the ServerRegistry interface.
func (endpoint ServerEndpoint) ListServers(ctx context.Context) ([]ServerEndpoint, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return []ServerEndpoint{endpoint}, nil
	}
}

// A ServerList represents a list of backend redis servers.
type ServerList []ServerEndpoint

// ListServers satisfies the ServerRegistry interface.
func (list ServerList) ListServers(ctx context.Context) ([]ServerEndpoint, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		res := make([]ServerEndpoint, len(list))
		copy(res, list)
		return res, nil
	}
}
