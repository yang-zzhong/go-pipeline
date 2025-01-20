package pipeline

import "errors"

func transfer[T1 any](input chan T1) func() ([]T1, bool, error) {
	return func() ([]T1, bool, error) {
		t2, ok := <-input
		if !ok {
			return nil, true, nil
		}
		return []T1{t2}, false, nil
	}
}

func transform[T1, T2 any](ch chan T2, transformFunc func([]T1) ([]T2, error)) func([]T1) error {
	return func(t2s []T1) error {
		if transformFunc == nil {
			close(ch)
			return errors.New("undefined transformFunc")
		}
		if len(t2s) == 0 {
			close(ch)
		}
		ret, err := transformFunc(t2s)
		if err != nil {
			close(ch)
			return err
		}
		for _, ti := range ret {
			ch <- ti
		}
		return nil
	}

}
