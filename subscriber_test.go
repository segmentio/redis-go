package redis

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSubscribe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	transport := &Transport{}

	sub, err := transport.Subscribe(ctx, "tcp", "localhost:6379", "A", "B", "C")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	deadline, _ := ctx.Deadline()
	sub.SetDeadline(deadline)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		client := &Client{Transport: transport}

		publish := []struct {
			channel string
			message string
		}{
			{"A", "1"},
			{"A", "2"},
			{"A", "3"},
			{"C", "Hello World!"},
		}

		for _, p := range publish {
			if err := client.Exec(ctx, "PUBLISH", p.channel, p.message); err != nil {
				t.Fatal(err)
			}
		}
	}()

	received := map[string][]string{}
	expected := map[string][]string{"A": {"1", "2", "3"}, "C": {"Hello World!"}}

	for i := 0; i != 4; i++ {
		channel, message, err := sub.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		received[channel] = append(received[channel], string(message))
	}

	if !reflect.DeepEqual(expected, received) {
		t.Error("bad messages received:")
		t.Logf("expected: %#v", expected)
		t.Logf("received: %#v", received)
	}

	wg.Wait()
}
