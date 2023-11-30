package cache_test

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/cache"
)

func expectValue(t *testing.T, c cache.Cache[int, string], key int) string {
	v, ok := c.Get(key)
	if !ok {
		t.Fatalf("missing value for key %v", key)
	}
	return v
}

func TestCacheMaxSize(t *testing.T) {
	c := cache.New[int, string](3)
	c.Put(1, "foo")
	c.Put(2, "bar")
	c.Put(3, "qux")
	c.Put(2, "baz")
	if c.Size() != 3 {
		t.Fatalf("got: %v want: 3", c.Size())
	}
	v := expectValue(t, c, 1)
	if v != "foo" {
		t.Fatalf("got: %v want: %v", v, "foo")
	}
	v = expectValue(t, c, 2)
	if v != "baz" {
		t.Fatalf("got: %v want: %v", v, "baz")
	}
	v = expectValue(t, c, 3)
	if v != "qux" {
		t.Fatalf("got: %v want: %v", v, "qux")
	}
	c.Put(4, "thud")
	if c.Size() != 3 {
		t.Fatalf("got: %v want: 3", c.Size())
	}
	if _, ok := c.Get(1); ok {
		t.Fatalf("expected evicted entry for %v", 1)
	}
	v = expectValue(t, c, 2)
	if v != "baz" {
		t.Fatalf("got: %v want: %v", v, "baz")
	}
	v = expectValue(t, c, 3)
	if v != "qux" {
		t.Fatalf("got: %v want: %v", v, "qux")
	}
	v = expectValue(t, c, 4)
	if v != "thud" {
		t.Fatalf("got: %v want: %v", v, "thud")
	}
}

func TestCacheLRU(t *testing.T) {
	c := cache.New[int, string](3)
	c.Put(1, "foo")
	c.Put(2, "bar")
	c.Put(3, "qux")
	v := expectValue(t, c, 1)
	if v != "foo" {
		t.Fatalf("got: %v want: %v", v, "baz")
	}
	// this should evict key 2 because it's LRU
	c.Put(4, "baz")
	if c.Size() != 3 {
		t.Fatalf("got: %v want: 3", c.Size())
	}
	if _, ok := c.Get(2); ok {
		t.Fatalf("got: %v want: false", ok)
	}
	v = expectValue(t, c, 1)
	if v != "foo" {
		t.Fatalf("got: %v want: %v", v, "baz")
	}
	v = expectValue(t, c, 3)
	if v != "qux" {
		t.Fatalf("got: %v want: %v", v, "qux")
	}
	v = expectValue(t, c, 4)
	if v != "baz" {
		t.Fatalf("got: %v want: %v", v, "thud")
	}
}
