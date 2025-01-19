package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"math/rand"

	"github.com/stretchr/testify/assert"
	"github.com/yang-zzhong/go-pipeline"
)

type stri struct {
	i int
	s string
}

func TestPipeline_generate_error(t *testing.T) {
	p := pipeline.New2[int, string]()
	p.Start(func() ([]int, bool, error) {
		return nil, false, errors.New("generate error")
	}).Next1(func(r []int) ([]string, error) {
		ret := []string{}
		for _, i := range r {
			time.Sleep(time.Millisecond * time.Duration(rand.Int63n(10)) * 10)
			s := fmt.Sprintf("%d", i)
			fmt.Printf("string: %s\n", s)
			ret = append(ret, s)
		}
		return ret, nil
	}, 10).Next2(func(r []string) ([]pipeline.E, error) {
		for _, s := range r {
			i, _ := strconv.Atoi(s)
			item := stri{i: i, s: s}
			fmt.Printf("complex: %v\n", item)
		}
		return nil, nil
	}, 20)

	err := p.Do(context.Background())
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "generate error")
}

func TestPipeline_next_error(t *testing.T) {
	total := 100
	offset := 0
	p := pipeline.New2[int, string]()
	p.Start(func() ([]int, bool, error) {
		start := offset
		end := offset + 1
		if end > total {
			end = total
		}
		ret := []int{}
		for i := start; i < end; i++ {
			ret = append(ret, i)
		}
		offset += len(ret)
		return ret, end == total, nil
	}).Next1(func(r []int) ([]string, error) {
		return nil, errors.New("next1 error")
	}, 10).Next2(func(r []string) ([]pipeline.E, error) {
		for _, s := range r {
			i, _ := strconv.Atoi(s)
			item := stri{i: i, s: s}
			fmt.Printf("complex: %v\n", item)
		}
		return nil, nil
	}, 20)

	err := p.Do(context.Background())
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "next1 error")
}

func TestPipeline(t *testing.T) {
	total := 100
	offset := 0
	p := pipeline.New2[int, string]()
	p.Start(func() ([]int, bool, error) {
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
		return ret, end == total, nil
	}).Next1(func(r []int) ([]string, error) {
		ret := []string{}
		for _, i := range r {
			time.Sleep(time.Millisecond * time.Duration(rand.Int63n(10)) * 10)
			s := fmt.Sprintf("%d", i)
			fmt.Printf("string: %s\n", s)
			ret = append(ret, s)
		}
		return ret, nil
	}, 10).Next2(func(r []string) ([]pipeline.E, error) {
		for _, s := range r {
			i, _ := strconv.Atoi(s)
			item := stri{i: i, s: s}
			fmt.Printf("complex: %v\n", item)
		}
		return nil, nil
	}, 20)

	p.Do(context.Background())
}
