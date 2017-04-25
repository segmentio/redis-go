package redis

import "sync"

type quetex struct {
	mutex sync.Mutex
	count int
	locks [](chan<- struct{})
}

func (q *quetex) acquire() <-chan struct{} {
	lock := make(chan struct{})
	q.mutex.Lock()

	if q.count == 0 {
		close(lock)
	} else {
		q.locks = append(q.locks, lock)
	}

	q.count++
	q.mutex.Unlock()
	return lock
}

func (q *quetex) release() {
	q.mutex.Lock()

	if q.count--; q.count != 0 {
		close(q.locks[0])
		q.locks = q.locks[1:]
	}

	q.mutex.Unlock()
}
