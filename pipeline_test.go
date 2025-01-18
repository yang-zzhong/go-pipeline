package pipeline_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"math/rand"

	"github.com/yang-zzhong/go-pipeline"
)

type stri struct {
	i int
	s string
}

func TestPipeline(t *testing.T) {
	total := 100
	offset := 0
	p := pipeline.New2[int, string]()
	p.Start(func() ([]int, bool) {
		start := offset
		end := offset + 1
		if end > total {
			end = total
		}
		ret := []int{}
		for i := start; i < end; i++ {
			time.Sleep(time.Millisecond * time.Duration(rand.Int63n(10)) * 10)
			fmt.Printf("number: %d\n", i)
			ret = append(ret, i)
		}
		offset += len(ret)
		return ret, end == total
	}).Next1(func(r []int) []string {
		ret := []string{}
		for _, i := range r {
			s := fmt.Sprintf("%d", i)
			fmt.Printf("string: %s\n", s)
			ret = append(ret, s)
		}
		return ret
	}, 10).Next2(func(r []string) []pipeline.E {
		for _, s := range r {
			i, _ := strconv.Atoi(s)
			item := stri{i: i, s: s}
			fmt.Printf("complex: %v\n", item)
		}
		return nil
	}, 20)

	p.Do(context.Background())
}
