package pipeline

import (
	"context"
	"fmt"
	"strconv"
	"testing"
)

type stri struct {
	i int
	s string
}

func TestPipeline(t *testing.T) {
	total := 10
	offset := 0
	p := NewPipeline2[int, string]()
	p.Start(func() ([]int, bool) {
		start := offset
		end := offset + 1
		if end > total {
			end = total
		}
		ret := []int{}
		for i := start; i < end; i++ {
			fmt.Printf("number: %d\n", i)
			ret = append(ret, i)
		}
		offset += len(ret)
		return ret, end == total
	}).With1(func(r []int) []string {
		ret := []string{}
		for _, i := range r {
			s := fmt.Sprintf("%d", i)
			fmt.Printf("string: %s\n", s)
			ret = append(ret, s)
		}
		return ret
	}, 2).With2(func(r []string) []E {
		for _, s := range r {
			i, _ := strconv.Atoi(s)
			item := stri{i: i, s: s}
			fmt.Printf("complex: %v\n", item)
		}
		return nil
	}, 3)

	p.Do(context.Background())
}
