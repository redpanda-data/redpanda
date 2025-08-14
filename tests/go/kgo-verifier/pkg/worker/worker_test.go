package worker

import (
	"math/rand"
	"strings"
	"testing"
)

type state struct {
	data []byte
}

func newstate(size int) state {
	data := make([]byte, size)
	for i := range data {
		// printable ascii range
		data[i] = byte(rand.Intn(126+1-32) + 32)
	}
	return state{
		data: data,
	}
}

func (s *state) generate(size, frag_size int) []byte {
	res := make([]byte, 0, size)
	for {
		remaining := size - len(res)
		if remaining == 0 {
			break
		}
		start := rand.Intn(size)
		view := s.data[start:]
		end := min(len(view), remaining, frag_size)
		res = append(res, view[:end]...)
	}
	return res
}

func benchmark_old_random_payload(i int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		randBytes := make([]byte, i)
		// An incompressible high entropy payload. This will likely not be UTF-8 decodable.
		n, err := rand.Read(randBytes)
		if err != nil {
			panic(err.Error())
		}
		if n != int(i) {
			panic("Unexpected byte count from rand.Read")
		}
		// Convert to a valid UTF-8 string, replacing bad chars with " ".
		// A valid UTF-8 string is needed to avoid any decoding issues
		// for services on the consuming end.
		payload := []byte(strings.ToValidUTF8(string(randBytes), " "))

		// In converting to valid UTF-8, we may have lost some bytes.
		// Append back the difference.
		diff := int(i) - len(payload)
		if diff > 0 {
			payload = append(payload, make([]byte, diff)...)
		}
	}
}

func benchmark_random_payload(i int, b *testing.B) {
	s := newstate(1 << 22)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s.generate(i, 1024)
	}
}

func benchmark_empty_payload(i int, b *testing.B) {
	gen := func() []byte {
		return make([]byte, i)
	}
	for n := 0; n < b.N; n++ {
		gen()
	}
}

func Benchmark_old_random_payload1(b *testing.B)  { benchmark_old_random_payload(10, b) }
func Benchmark_old_random_payload2(b *testing.B)  { benchmark_old_random_payload(100, b) }
func Benchmark_old_random_payload3(b *testing.B)  { benchmark_old_random_payload(1000, b) }
func Benchmark_old_random_payload10(b *testing.B) { benchmark_old_random_payload(10000, b) }
func Benchmark_old_random_payload20(b *testing.B) { benchmark_old_random_payload(100000, b) }
func Benchmark_old_random_payload40(b *testing.B) { benchmark_old_random_payload(1000000, b) }

func Benchmark_random_payload1(b *testing.B)  { benchmark_random_payload(10, b) }
func Benchmark_random_payload2(b *testing.B)  { benchmark_random_payload(100, b) }
func Benchmark_random_payload3(b *testing.B)  { benchmark_random_payload(1000, b) }
func Benchmark_random_payload10(b *testing.B) { benchmark_random_payload(10000, b) }
func Benchmark_random_payload20(b *testing.B) { benchmark_random_payload(100000, b) }
func Benchmark_random_payload40(b *testing.B) { benchmark_random_payload(1000000, b) }

func Benchmark_empty_payload1(b *testing.B)  { benchmark_empty_payload(10, b) }
func Benchmark_empty_payload2(b *testing.B)  { benchmark_empty_payload(100, b) }
func Benchmark_empty_payload3(b *testing.B)  { benchmark_empty_payload(1000, b) }
func Benchmark_empty_payload10(b *testing.B) { benchmark_empty_payload(10000, b) }
func Benchmark_empty_payload20(b *testing.B) { benchmark_empty_payload(100000, b) }
func Benchmark_empty_payload40(b *testing.B) { benchmark_empty_payload(1000000, b) }
