package sandbox

import "time"

type logEntry struct {
	timestamp time.Time
	nodeID    int
	line      string
}

type logEntriesHeap []logEntry

func (h logEntriesHeap) Len() int {
	return len(h)
}
func (h logEntriesHeap) Less(i, j int) bool {
	return h[i].timestamp.Unix() < h[j].timestamp.Unix()
}
func (h logEntriesHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *logEntriesHeap) Push(x interface{}) {
	*h = append(*h, x.(logEntry))
}

func (h *logEntriesHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
