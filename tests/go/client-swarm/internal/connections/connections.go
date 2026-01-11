package connections

import (
	"context"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Options for connection test configuration
type Options struct {
	Brokers []string
	Number  int
}

// Stats contains connection test statistics
type Stats struct {
	TotalAttempted int64
	Successful     int64
	Failed         int64
	FailedWrites   int64
}

// Run runs the connection stress test
func Run(ctx context.Context, opts Options) (*Stats, error) {
	if len(opts.Brokers) == 0 {
		return nil, nil
	}

	var stats Stats
	stats.TotalAttempted = int64(opts.Number)

	var wg sync.WaitGroup
	var successful, failed, failedWrites int64

	log.Printf("Starting %d connections to %v", opts.Number, opts.Brokers)

	for i := 0; i < opts.Number; i++ {
		wg.Add(1)
		brokerIdx := i % len(opts.Brokers)
		broker := opts.Brokers[brokerIdx]

		go func(id int, addr string) {
			defer wg.Done()

			select {
			case <-ctx.Done():
				return
			default:
			}

			// Connect
			conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
			if err != nil {
				atomic.AddInt64(&failed, 1)
				if failed < 10 || failed%100 == 0 {
					log.Printf("Connection %d: failed to connect to %s: %v", id, addr, err)
				}
				return
			}
			defer conn.Close()

			atomic.AddInt64(&successful, 1)

			// Send some data packets
			data := make([]byte, 1024)
			for j := 0; j < 1000; j++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				_, err := conn.Write(data)
				if err != nil {
					atomic.AddInt64(&failedWrites, 1)
					if failedWrites < 10 || failedWrites%100 == 0 {
						log.Printf("Connection %d: write error: %v", id, err)
					}
					return
				}

				// Small delay between writes
				time.Sleep(time.Millisecond)
			}
		}(i, broker)
	}

	wg.Wait()

	stats.Successful = atomic.LoadInt64(&successful)
	stats.Failed = atomic.LoadInt64(&failed)
	stats.FailedWrites = atomic.LoadInt64(&failedWrites)

	return &stats, nil
}
