package redis

import "testing"

func TestQuetex(t *testing.T) {
	q := quetex{}

	lock1 := q.acquire()
	lock2 := q.acquire()
	lock3 := q.acquire()

	select {
	case <-lock1:
		q.release()
	case <-lock2:
		t.Error("order mismatch")
	case <-lock3:
		t.Error("order mismatch")
	}

	select {
	case <-lock2:
		q.release()
	case <-lock3:
		t.Error("order mismatch")
	}

	<-lock3
	q.release()
}
