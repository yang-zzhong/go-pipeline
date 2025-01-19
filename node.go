package pipeline

import (
	"context"
	"errors"
	"sync"
)

type Node[T any] struct {
	q    chan T
	g    func() ([]T, bool, error)
	c    func(data []T) error
	size int
}

func (p Node[T]) Do(ctx context.Context) (err error) {
	var wg sync.WaitGroup
	var errlock sync.Mutex
	wg.Add(2)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				if e, ok := r.(error); ok {
					if e.Error() == "send on closed channel" {
						return
					}
					errlock.Lock()
					err = e
					errlock.Unlock()
					close(p.q)
					return
				}
				if s, ok := r.(string); ok {
					errlock.Lock()
					err = errors.New(s)
					errlock.Unlock()
					close(p.q)
					return
				}
				panic(r)
			}
		}()
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				close(p.q)
				return
			default:
				var (
					items []T
					done  bool
					e     error
				)
				if items, done, e = p.g(); e != nil {
					errlock.Lock()
					if err == nil {
						err = e
					}
					errlock.Unlock()
					close(p.q)
					return
				}
				for _, item := range items {
					p.q <- item
				}
				if done {
					close(p.q)
					return
				}
			}
		}
	}()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				if e, ok := r.(error); ok {
					errlock.Lock()
					err = e
					errlock.Unlock()
					close(p.q)
					return
				}
				if s, ok := r.(string); ok {
					errlock.Lock()
					err = errors.New(s)
					errlock.Unlock()
					close(p.q)
					return
				}
				panic(r)
			}
		}()
		defer wg.Done()
		group := []T{}
		for {
			select {
			case <-ctx.Done():
				if len(group) > 0 {
					if e := p.c(group); e != nil {
						errlock.Lock()
						if err == nil {
							err = e
						}
						errlock.Unlock()
						return
					}
				}
				p.c(nil)
				return
			case item, ok := <-p.q:
				if !ok {
					if len(group) > 0 {
						if e := p.c(group); e != nil {
							errlock.Lock()
							if err == nil {
								err = e
							}
							errlock.Unlock()
							close(p.q)
							return
						}
					}
					p.c(nil)
					return
				}
				group = append(group, item)
				if len(group) >= p.size {
					if e := p.c(group); e != nil {
						errlock.Lock()
						if err == nil {
							err = e
						}
						errlock.Unlock()
						close(p.q)
						return
					}
					group = []T{}
				}
			}
		}
	}()
	wg.Wait()
	return
}

func NewNode[T any](producer func() ([]T, bool, error), consumer func(data []T) error, size int) *Node[T] {
	return &Node[T]{
		q:    make(chan T),
		c:    consumer,
		g:    producer,
		size: size,
	}
}
