package maaq

import (
	"sync"
)

type doc map[string]interface{}

type maaqJob interface {
	do(d *doc) error
	started() (*doc, error)    // Job started
	failed() (*doc, error)     // Job failed
	successful() (*doc, error) // Job successful
}

func Produce(j maaqJob, done <-chan bool) <-chan *doc {
	out := make(chan *doc)
	go func() {
		for {
			select {
			case <-done:
				close(out)
				return
			default:
				v, err := j.started()
				if err != nil {
					continue
				}
				out <- v
			}
		}
	}()
	return out
}

func Consume(m maaqJob, in <-chan *doc, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case v, ok := <-in:
			if !ok {
				return
			}
			err := m.do(v)
			if err != nil {
				m.failed()
				continue
			}
			m.successful()
		}
	}
}
