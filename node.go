package pipeline

import (
	"context"
	"sync"
)

type Node[T any] struct {
	q    chan T
	g    func() ([]T, bool)
	c    func([]T)
	size int
}

func (p Node[T]) Do(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				close(p.q)
				return
			default:
				items, done := p.g()
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
		defer wg.Done()
		group := []T{}
		for {
			select {
			case <-ctx.Done():
				if len(group) > 0 {
					p.c(group)
				}
				p.c(nil)
				return
			case item, ok := <-p.q:
				if !ok {
					if len(group) > 0 {
						p.c(group)
					}
					p.c(nil)
					return
				}
				group = append(group, item)
				if len(group) >= p.size {
					p.c(group)
					group = []T{}
				}
			}
		}
	}()
	wg.Wait()
}

func NewNode[T any](producer func() ([]T, bool), consumer func([]T), size int) *Node[T] {
	return &Node[T]{
		q:    make(chan T),
		c:    consumer,
		g:    producer,
		size: size,
	}
}
