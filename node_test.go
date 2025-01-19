package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPipelineNode(t *testing.T) {
	total := 11
	offset := 0
	should := 0
	producer := func() ([]int, bool, error) {
		start := offset
		end := offset + 2
		if end > total {
			end = total
		}
		ret := []int{}
		for i := start; i < end; i++ {
			should += i
			ret = append(ret, i)
		}
		offset += 2
		return ret, end == total, nil
	}
	r := 0
	consumer := func(p []int) error {
		for _, i := range p {
			r += i
		}
		return nil
	}
	n := NewNode(producer, consumer, 1)
	n.Do(context.Background())
	assert.Equal(t, r, should, "result not equal")
}

func TestPipelineNode_error(t *testing.T) {
	total := 11
	offset := 0
	should := 0
	producer := func() ([]int, bool, error) {
		start := offset
		end := offset + 2
		if end > total {
			end = total
		}
		ret := []int{}
		for i := start; i < end; i++ {
			should += i
			ret = append(ret, i)
		}
		offset += 2
		return ret, end == total, nil
	}
	r := 0
	consumer := func(p []int) error {
		for _, i := range p {
			if i > 8 {
				return errors.New("gt 8")
			}
			r += i
		}
		return nil
	}
	n := NewNode(producer, consumer, 1)
	err := n.Do(context.Background())
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "gt 8")
}

func TestPipelineNode_panic(t *testing.T) {
	total := 11
	offset := 0
	should := 0
	producer := func() ([]int, bool, error) {
		start := offset
		end := offset + 2
		if end > total {
			end = total
		}
		ret := []int{}
		for i := start; i < end; i++ {
			should += i
			ret = append(ret, i)
		}
		offset += 2
		return ret, end == total, nil
	}
	consumer := func(p []int) error {
		panic("hello")
	}
	n := NewNode(producer, consumer, 1)
	err := n.Do(context.Background())
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "hello")
}
