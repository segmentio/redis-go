package redis

import "testing"

func TestGetKey(t *testing.T) {
	req1 := NewRequest("localhost:6379", "SET", List("foo{bar}", "42"))

	k1, ok1 := req1.getKey()
	if !ok1 {
		t.Errorf("got no key for %s", req1.Args)
	}

	if k1 != "bar" {
		t.Errorf("expected 1 got %s", k1)
	}

	req2 := NewRequest("localhost:6379", "SET", List("foo", "bar"))

	k2, ok2 := req2.getKey()
	if !ok2 {
		t.Errorf("got no key for %s", req2.Args)
	}

	if k2 != "foo" {
		t.Errorf("expected foo got %s", k2)
	}
}
