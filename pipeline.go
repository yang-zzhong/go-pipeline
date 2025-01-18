package pipeline

import (
	"context"
	"sync"
)

type Doer interface {
	Do(context.Context)
}

type Pipeline[T1, T2, T3, T4, T5, T6, T7, T8 any] struct {
	head        func() ([]T1, bool)
	sizes       []int
	transform_1 func([]T1) []T2
	transform_2 func([]T2) []T3
	transform_3 func([]T3) []T4
	transform_4 func([]T4) []T5
	transform_5 func([]T5) []T6
	transform_6 func([]T6) []T7
	transform_7 func([]T7) []T8
}

func NewPipeline1[T1 any]() *Pipeline[T1, struct{}, struct{}, struct{}, struct{}, struct{}, struct{}, struct{}] {
	return &Pipeline[T1, struct{}, struct{}, struct{}, struct{}, struct{}, struct{}, struct{}]{sizes: make([]int, 1)}
}

func NewPipeline2[T1, T2 any]() *Pipeline[T1, T2, struct{}, struct{}, struct{}, struct{}, struct{}, struct{}] {
	return &Pipeline[T1, T2, struct{}, struct{}, struct{}, struct{}, struct{}, struct{}]{sizes: make([]int, 2)}
}

func NewPipeline3[T1, T2, T3 any]() *Pipeline[T1, T2, T3, struct{}, struct{}, struct{}, struct{}, struct{}] {
	return &Pipeline[T1, T2, T3, struct{}, struct{}, struct{}, struct{}, struct{}]{sizes: make([]int, 3)}
}

func NewPipeline4[T1, T2, T3, T4 any]() *Pipeline[T1, T2, T3, T4, struct{}, struct{}, struct{}, struct{}] {
	return &Pipeline[T1, T2, T3, T4, struct{}, struct{}, struct{}, struct{}]{sizes: make([]int, 4)}
}

func NewPipeline5[T1, T2, T3, T4, T5 any]() *Pipeline[T1, T2, T3, T4, T5, struct{}, struct{}, struct{}] {
	return &Pipeline[T1, T2, T3, T4, T5, struct{}, struct{}, struct{}]{sizes: make([]int, 5)}
}

func NewPipeline6[T1, T2, T3, T4, T5, T6 any]() *Pipeline[T1, T2, T3, T4, T5, T6, struct{}, struct{}] {
	return &Pipeline[T1, T2, T3, T4, T5, T6, struct{}, struct{}]{sizes: make([]int, 6)}
}

func NewPipeline7[T1, T2, T3, T4, T5, T6, T7 any]() *Pipeline[T1, T2, T3, T4, T5, T6, T7, struct{}] {
	return &Pipeline[T1, T2, T3, T4, T5, T6, T7, struct{}]{sizes: make([]int, 7)}
}

func NewPipeline8[T1, T2, T3, T4, T5, T6, T7, T8 any]() *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	return &Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]{sizes: make([]int, 8)}
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) Start(head func() ([]T1, bool)) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.head = head
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) With1(transformer func([]T1) []T2, size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[0] = size
	p.transform_1 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) With2(transformer func([]T2) []T3, size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[1] = size
	p.transform_2 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) With3(transformer func([]T3) []T4, size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[2] = size
	p.transform_3 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) With4(transformer func([]T4) []T5, size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[3] = size
	p.transform_4 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) With5(transformer func([]T5) []T6, size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[4] = size
	p.transform_5 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) With6(transformer func([]T6) []T7, size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[5] = size
	p.transform_6 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) With7(transformer func([]T7) []T8, size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[6] = size
	p.transform_7 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) Do(ctx context.Context) {
	doers := []Doer{}
	var (
		ch2    chan T2
		ch3    chan T3
		ch4    chan T4
		ch5    chan T5
		ch6    chan T6
		ch7    chan T7
		ch8    chan T8
		chDone = []chan struct{}{}
	)
	for i := 0; i < len(p.sizes); i++ {
		if i == 0 {
			ch2 = make(chan T2, p.sizes[i])
			chDone = append(chDone, make(chan struct{}))
			doers = append(doers, NewNode(p.head, func(ts []T1) {
				if len(ts) == 0 || p.transform_1 == nil {
					if i < len(p.sizes)-1 {
						chDone[i] <- struct{}{}
					}
					return
				}
				ret := p.transform_1(ts)
				for _, ti := range ret {
					ch2 <- ti
				}
			}, p.sizes[i]))
		} else if i == 1 {
			ch3 = make(chan T3, p.sizes[i])
			chDone = append(chDone, make(chan struct{}))
			doers = append(doers, NewNode(func() ([]T2, bool) {
				select {
				case <-chDone[i-1]:
					if i < len(p.sizes)-1 {
						chDone[i] <- struct{}{}
					}
					return nil, true
				case t2 := <-ch2:
					return []T2{t2}, false
				}
			}, func(t2s []T2) {
				if p.transform_2 == nil {
					if i < len(p.sizes)-1 {
						chDone[i] <- struct{}{}
					}
					return
				}
				t3s := p.transform_2(t2s)
				for _, ti := range t3s {
					ch3 <- ti
				}
			}, p.sizes[i]))
		} else if i == 2 {
			ch4 = make(chan T4, p.sizes[i])
			chDone = append(chDone, make(chan struct{}))
			doers = append(doers, NewNode(func() ([]T3, bool) {
				select {
				case <-chDone[i-1]:
					if i < len(p.sizes)-1 {
						chDone[i] <- struct{}{}
					}
					return nil, true
				case t2 := <-ch3:
					return []T3{t2}, false
				}
			}, func(t2s []T3) {
				if p.transform_3 == nil {
					if i < len(p.sizes)-1 {
						chDone[i] <- struct{}{}
					}
					return
				}
				t4s := p.transform_3(t2s)
				for _, ti := range t4s {
					ch4 <- ti
				}
			}, p.sizes[i]))
		} else if i == 3 {
			ch5 = make(chan T5, p.sizes[i])
			chDone = append(chDone, make(chan struct{}))
			doers = append(doers, NewNode(func() ([]T4, bool) {
				select {
				case <-chDone[i-1]:
					if i < len(p.sizes)-1 {
						chDone[i] <- struct{}{}
					}
					return nil, true
				case t2 := <-ch4:
					return []T4{t2}, false
				}
			}, func(t2s []T4) {
				if p.transform_4 == nil {
					if i < len(p.sizes)-1 {
						chDone[i] <- struct{}{}
					}
					return
				}
				t4s := p.transform_4(t2s)
				for _, ti := range t4s {
					ch5 <- ti
				}
			}, p.sizes[i]))
		} else if i == 4 {
			ch6 = make(chan T6, p.sizes[i])
			chDone = append(chDone, make(chan struct{}))
			doers = append(doers, NewNode(func() ([]T5, bool) {
				select {
				case <-chDone[i-1]:
					if i < len(p.sizes)-1 {
						chDone[i] <- struct{}{}
					}
					return nil, true
				case t2 := <-ch5:
					return []T5{t2}, false
				}
			}, func(t2s []T5) {
				if p.transform_5 == nil {
					if i < len(p.sizes)-1 {
						chDone[i] <- struct{}{}
					}
					return
				}
				t4s := p.transform_5(t2s)
				for _, ti := range t4s {
					ch6 <- ti
				}
			}, p.sizes[i]))
		} else if i == 5 {
			ch7 = make(chan T7, p.sizes[i])
			chDone = append(chDone, make(chan struct{}))
			doers = append(doers, NewNode(func() ([]T6, bool) {
				select {
				case <-chDone[i-1]:
					if i < len(p.sizes)-1 {
						chDone[i] <- struct{}{}
					}
					return nil, true
				case t2 := <-ch6:
					return []T6{t2}, false
				}
			}, func(t2s []T6) {
				if p.transform_6 == nil {
					if i < len(p.sizes)-1 {
						chDone[i] <- struct{}{}
					}
					return
				}
				t4s := p.transform_6(t2s)
				for _, ti := range t4s {
					ch7 <- ti
				}
			}, p.sizes[i]))
		} else if i == 6 {
			ch8 = make(chan T8, p.sizes[i])
			chDone = append(chDone, make(chan struct{}))
			doers = append(doers, NewNode(func() ([]T7, bool) {
				select {
				case <-chDone[i-1]:
					if i < len(p.sizes)-1 {
						chDone[i] <- struct{}{}
					}
					return nil, true
				case t2 := <-ch7:
					return []T7{t2}, false
				}
			}, func(t2s []T7) {
				if p.transform_7 == nil {
					if i < len(p.sizes)-1 {
						chDone[i] <- struct{}{}
					}
					return
				}
				t4s := p.transform_7(t2s)
				for _, ti := range t4s {
					ch8 <- ti
				}
			}, p.sizes[i]))
		}
	}
	var wg sync.WaitGroup
	for _, doer := range doers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			doer.Do(ctx)
		}()
	}
	wg.Wait()
}
