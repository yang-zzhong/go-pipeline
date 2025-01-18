package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPipelineNode(t *testing.T) {
	total := 10000
	offset := 0
	should := 0
	producer := func() ([]int, bool) {
		start := offset
		end := offset + 1000
		if end > total {
			end = total
		}
		ret := []int{}
		for i := start; i < end; i++ {
			should += i
			ret = append(ret, i)
		}
		return ret, end == total
	}
	r := 0
	consumer := func(p []int) {
		for _, i := range p {
			r += i
		}
		offset += len(p)
	}
	n := NewNode(producer, consumer, 1000)
	n.Do(context.Background())
	assert.Equal(t, r, should, "result not equal")
}
