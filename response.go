package redis

// Response represents the response from a Redis request.
type Response struct {
	// Args is the arguments list of the response.
	//
	// When the response is obtained from Transport.RoundTrip or from Client.Do
	// the Args field is never nil.
	Args Args

	// TxArgs is the argument list of response to requests that were sent as
	// transactions.
	TxArgs TxArgs

	// Request is the request that was sent to obtain this Response.
	Request *Request
}

// Close closes all arguments of the response.
func (res *Response) Close() error {
	var err error

	if res.Args != nil {
		err = res.Args.Close()
	}

	if res.TxArgs != nil {
		err = res.TxArgs.Close()
	}

	return err
}
