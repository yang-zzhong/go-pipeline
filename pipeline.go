package pipeline

import (
	"context"
	"errors"
	"sync"
)

type Doer interface {
	Do(context.Context) error
}

type E struct{}

type Pipeline[T1, T2, T3, T4, T5, T6, T7, T8 any] struct {
	generate    func() ([]T1, bool)
	sizes       []int
	transform_1 func([]T1) ([]T2, error)
	transform_2 func([]T2) ([]T3, error)
	transform_3 func([]T3) ([]T4, error)
	transform_4 func([]T4) ([]T5, error)
	transform_5 func([]T5) ([]T6, error)
	transform_6 func([]T6) ([]T7, error)
	transform_7 func([]T7) ([]T8, error)
}

func New1[T1 any]() *Pipeline[T1, E, E, E, E, E, E, E] {
	return &Pipeline[T1, E, E, E, E, E, E, E]{sizes: make([]int, 1)}
}

func New2[T1, T2 any]() *Pipeline[T1, T2, E, E, E, E, E, E] {
	return &Pipeline[T1, T2, E, E, E, E, E, E]{sizes: make([]int, 2)}
}

func New3[T1, T2, T3 any]() *Pipeline[T1, T2, T3, E, E, E, E, E] {
	return &Pipeline[T1, T2, T3, E, E, E, E, E]{sizes: make([]int, 3)}
}

func New4[T1, T2, T3, T4 any]() *Pipeline[T1, T2, T3, T4, E, E, E, E] {
	return &Pipeline[T1, T2, T3, T4, E, E, E, E]{sizes: make([]int, 4)}
}

func New5[T1, T2, T3, T4, T5 any]() *Pipeline[T1, T2, T3, T4, T5, E, E, E] {
	return &Pipeline[T1, T2, T3, T4, T5, E, E, E]{sizes: make([]int, 5)}
}

func New6[T1, T2, T3, T4, T5, T6 any]() *Pipeline[T1, T2, T3, T4, T5, T6, E, E] {
	return &Pipeline[T1, T2, T3, T4, T5, T6, E, E]{sizes: make([]int, 6)}
}

func New7[T1, T2, T3, T4, T5, T6, T7 any]() *Pipeline[T1, T2, T3, T4, T5, T6, T7, E] {
	return &Pipeline[T1, T2, T3, T4, T5, T6, T7, E]{sizes: make([]int, 7)}
}

func New8[T1, T2, T3, T4, T5, T6, T7, T8 any]() *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	return &Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]{sizes: make([]int, 8)}
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) Start(generate func() ([]T1, bool)) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.generate = generate
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) Next1(transformer func([]T1) ([]T2, error), size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[0] = size
	p.transform_1 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) Next2(transformer func([]T2) ([]T3, error), size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[1] = size
	p.transform_2 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) Next3(transformer func([]T3) ([]T4, error), size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[2] = size
	p.transform_3 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) Next4(transformer func([]T4) ([]T5, error), size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[3] = size
	p.transform_4 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) Next5(transformer func([]T5) ([]T6, error), size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[4] = size
	p.transform_5 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) Next6(transformer func([]T6) ([]T7, error), size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[5] = size
	p.transform_6 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) Next7(transformer func([]T7) ([]T8, error), size int) *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8] {
	p.sizes[6] = size
	p.transform_7 = transformer
	return p
}

func (p *Pipeline[T1, T2, T3, T4, T5, T6, T7, T8]) Do(ctx context.Context) error {
	doers := []Doer{}
	var (
		ch2 chan T2
		ch3 chan T3
		ch4 chan T4
		ch5 chan T5
		ch6 chan T6
		ch7 chan T7
		ch8 chan T8
	)
	for i := 0; i < len(p.sizes); i++ {
		if i == 0 {
			ch2 = make(chan T2, p.sizes[i])
			doers = append(doers, NewNode(p.generate, func(ts []T1) error {
				if len(ts) == 0 || p.transform_1 == nil {
					if i < len(p.sizes)-1 {
						close(ch2)
					}
					if p.transform_1 == nil {
						return errors.New("undefined transform_1")
					}
					return nil
				}
				ret, err := p.transform_1(ts)
				if err != nil {
					if i < len(p.sizes)-1 {
						close(ch2)
					}
					return err
				}
				for _, ti := range ret {
					ch2 <- ti
				}
				return nil
			}, p.sizes[i]))
		} else if i == 1 {
			ch3 = make(chan T3, p.sizes[i])
			doers = append(doers, NewNode(func() ([]T2, bool) {
				t2, ok := <-ch2
				if !ok {
					return nil, true
				}
				return []T2{t2}, false
			}, func(t2s []T2) error {
				if p.transform_2 == nil {
					if i < len(p.sizes)-1 {
						close(ch3)
					}
					return errors.New("undefined transform_2")
				}
				t3s, err := p.transform_2(t2s)
				if err != nil {
					if i < len(p.sizes)-1 {
						close(ch2)
					}
					return err
				}
				for _, ti := range t3s {
					ch3 <- ti
				}
				return nil
			}, p.sizes[i]))
		} else if i == 2 {
			ch4 = make(chan T4, p.sizes[i])
			doers = append(doers, NewNode(func() ([]T3, bool) {
				t2, ok := <-ch3
				if !ok {
					return nil, true
				}
				return []T3{t2}, false
			}, func(t2s []T3) error {
				if p.transform_3 == nil {
					if i < len(p.sizes)-1 {
						close(ch4)
					}
					return errors.New("undefined transform_3")
				}
				t4s, err := p.transform_3(t2s)
				if err != nil {
					if i < len(p.sizes)-1 {
						close(ch2)
					}
					return err
				}
				for _, ti := range t4s {
					ch4 <- ti
				}
				return nil
			}, p.sizes[i]))
		} else if i == 3 {
			ch5 = make(chan T5, p.sizes[i])
			doers = append(doers, NewNode(func() ([]T4, bool) {
				t2, ok := <-ch4
				if !ok {
					return nil, true
				}
				return []T4{t2}, false
			}, func(t2s []T4) error {
				if p.transform_4 == nil {
					if i < len(p.sizes)-1 {
						close(ch5)
					}
					return errors.New("undefined transform_4")
				}
				t4s, err := p.transform_4(t2s)
				if err != nil {
					if i < len(p.sizes)-1 {
						close(ch5)
					}
					return err
				}
				for _, ti := range t4s {
					ch5 <- ti
				}
				return nil
			}, p.sizes[i]))
		} else if i == 4 {
			ch6 = make(chan T6, p.sizes[i])
			doers = append(doers, NewNode(func() ([]T5, bool) {
				t2, ok := <-ch5
				if !ok {
					return nil, true
				}
				return []T5{t2}, false
			}, func(t2s []T5) error {
				if p.transform_5 == nil {
					if i < len(p.sizes)-1 {
						close(ch6)
					}
					return errors.New("undefined transform_5")
				}
				t4s, err := p.transform_5(t2s)
				if err != nil {
					if i < len(p.sizes)-1 {
						close(ch5)
					}
					return err
				}
				for _, ti := range t4s {
					ch6 <- ti
				}
				return nil
			}, p.sizes[i]))
		} else if i == 5 {
			ch7 = make(chan T7, p.sizes[i])
			doers = append(doers, NewNode(func() ([]T6, bool) {
				t2, ok := <-ch6
				if !ok {
					return nil, true
				}
				return []T6{t2}, false
			}, func(t2s []T6) error {
				if p.transform_6 == nil {
					if i < len(p.sizes)-1 {
						close(ch7)
					}
					return errors.New("undefined transform_6")
				}
				t4s, err := p.transform_6(t2s)
				if err != nil {
					if i < len(p.sizes)-1 {
						close(ch5)
					}
					return err
				}
				for _, ti := range t4s {
					ch7 <- ti
				}
				return nil
			}, p.sizes[i]))
		} else if i == 6 {
			ch8 = make(chan T8, p.sizes[i])
			doers = append(doers, NewNode(func() ([]T7, bool) {
				t2, ok := <-ch7
				if !ok {
					return nil, true
				}
				return []T7{t2}, false
			}, func(t2s []T7) error {
				if p.transform_7 == nil {
					if i < len(p.sizes)-1 {
						close(ch8)
					}
					return errors.New("undefined transform_7")
				}
				t4s, err := p.transform_7(t2s)
				if err != nil {
					if i < len(p.sizes)-1 {
						close(ch5)
					}
					return err
				}
				for _, ti := range t4s {
					ch8 <- ti
				}
				return nil
			}, p.sizes[i]))
		}
	}
	var wg sync.WaitGroup
	var errs Error
	var errlock sync.Mutex
	for _, doer := range doers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := doer.Do(ctx); err != nil {
				errlock.Lock()
				errs = append(errs, err)
				errlock.Unlock()
			}
		}()
	}
	wg.Wait()
	return errs
}
