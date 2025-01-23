![Illustration](https://github.com/yang-zzhong/go-pipeline/blob/master/illustration.png?raw=true)

```go
import (
	"context"
	"fmt"
	"strconv"
	"time"
	"math/rand"
	"github.com/yang-zzhong/go-pipeline"
)

func main() {
	total := 100
	offset := 0
	p := pipeline.New4[int, int, int, string]()
    // generate start data with Start func 
    //  -> Next1 with 5 of chan cache
    //  -> Next2 with 5 of chan cache 
    //  -> Next3 with 10 of chan cache
    //  -> Next4 with 20 of chan cache
    //
    // any of cache if full, after operation will block, this feature should limit the resource use of the program
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
	}).Next1(func(r []int) ([]int, error) {
		return r, nil
	}, 5).Next2(func(r []int) ([]int, error) {
		return r, nil
	}, 5).Next3(func(r []int) ([]string, error) {
		ret := []string{}
		for _, i := range r {
			time.Sleep(time.Millisecond * time.Duration(rand.Int63n(10)) * 10)
			s := fmt.Sprintf("%d", i)
			fmt.Printf("string: %s\n", s)
			ret = append(ret, s)
		}
		return ret, nil
	}, 10).Next4(func(r []string) ([]pipeline.E, error) {
		for _, s := range r {
			i, _ := strconv.Atoi(s)
			item := stri{i: i, s: s}
			fmt.Printf("complex: %v\n", item)
		}
		return nil, nil
	}, 20)

	p.Do(context.Background())
}
```