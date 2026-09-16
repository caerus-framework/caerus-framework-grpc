package cf_grpc

import "google.golang.org/grpc"

// Stub builds a typed gRPC client stub from a Client's live Conn facade.
// Prefer storing the *Client peer and calling this (or NewXxxClient) once at
// Init — the facade keeps Invoke/NewStream on the current connection after reload.
func Stub[T any](c *Client, newFn func(grpc.ClientConnInterface) T) T {
	return newFn(c.Conn())
}
