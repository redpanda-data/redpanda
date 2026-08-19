# TLDR:

 - Write sync code if possible
 - Use coroutines over continuations
 - Don't overoptimize for allocs in microbenchmarks unless the per coroutine
   work is very low, e.g.: when parsing fields on a per record level

# Writing high performance coroutine code

## Keep coroutine chains intact aka. just use coroutines

HALO (heap allocation elision optimization) can merge nested coroutine frames
into the parent frame. This works best if all code is coroutines. coroutine ->
continuation -> coroutine -> continuation doesn't allow for frame merging. Note
that there is no magical optimizer that will help with this. 

**Do:** Keep async call chains coroutine based all the way down.

```cpp
ss::future<> a() {
  co_await b();
}

ss::future<> b() {
  co_await c();
}
```

**Don't:** Mix coroutines and continuations 

```cpp
ss::future<> a() {
    co_await b().then(...)
}
```

## Await futures directly

If the returned future of a coroutine is not needed for async purposes
`co_await` a coroutine directly. Otherwise HALO won't apply.

**Do:** 

```cpp
co_await foo();
```

**Don't:** 

```cpp
auto f = foo();
co_await std::move(f);
```

## Pass futures directly

`when_all`, `when_all_succeed`, `coroutine::as_future`, `coroutine::try_future`
and `coroutine::without_preemption_check` are HALO aware.

**Do:** Pass newly created coroutine futures directly to these wrappers.

```cpp
co_await ss::when_all(a(), b());
co_await ss::coroutine::as_future(c());
```

**Don't:** Store the child future first when it can be passed directly.

```cpp
auto f = c();
co_await ss::coroutine::as_future(std::move(f));
```

## Be aware of scheduling semantics

The biggest functional and also performance difference between continuations and
coroutines is their scheduling semantics.

`co_await` is a preemption point.

`.then` doesn't perform the same preemption check and future returning callbacks
use Seastar's urgent scheduling (in contrast to value returning callbacks that
use normal append-to-end-of-taskqueue scheduling).

**Do:** Make an intentional choice when replacing continuation code.

```cpp
co_await ss::coroutine::without_preemption_check(work());
update();
```

**Don't:** Assume a direct rewrite is scheduling equivalent. This can change
lock hold time, batching and latency.

```cpp
future<> work();

future<> cont() {
    return get_lock().then([] {  return work(); });
}

// not-equivalent
future<> coro() {
    auto lock = co_await get_lock();
    co_await work();
}
```

The coroutine version can potentially suspend twice, once just after acquiring
the lock and a second time before calling `work`.

The continuation version never suspends under lock. This isn't just about
correctness (most of the time it's fine to suspend under the lock) but also a
performance one.

Note that neither is directly better or worse. Suspending under the lock is
likely bad for latency but can cause batching in other parts of the system. This
is in all likelihood good for throughput and reduced reactor util. If you want
batching it's best to implement an explicit batching pattern that doesn't
rely on scheduling semantics.

## Don't overoptimize for allocations

Don't obsess over allocations related to coroutines and continuations. They only
matter if the per coroutine/continuation work is very small. There are very few
cases in Redpanda that fit this pattern, examples are: field parsing in
iceberg/protobuf/json/avro, per field compression handling etc. In the big
picture it makes very little difference CPU wise as most of the time the
work-per-coroutine is high.

LLMs love reducing allocs via coroutine/continuation changes as the allocs
number in microbenches is stable and via such optimizations is easy to reduce
(at the cost of readability/clarity).


## Write HALO aware wrappers

Constructor arguments don't currently propagate a safe elide context.

**Do:** Use a factory function and consider `coro_await_elidable_argument` when
the child is guaranteed to finish before the wrapper does.

```cpp
auto handle(SEASTAR_CORO_AWAIT_ELIDABLE_ARGUMENT ss::future<>&& f) {
    ...
    co_await f;
    ...
}
```

**Don't:** Add the attribute if the child can escape the wrapper. It is a
lifetime contract, not just an optimization hint.

```cpp
auto detach(SEASTAR_CORO_AWAIT_ELIDABLE_ARGUMENT ss::future<>&& f) {
    ...
    background(std::move(f)); // Wrong: f escapes the wrapper.
    ...
}
```

Again keep in mind that this level of tuning is only relevant for very commonly
used helper functions.

## Avoid recursive coroutine chains

The compiler can't embed a potentially unbounded number of frames.

**Do:** Use an iterative coroutine where practical.

```cpp
ss::future<> visit(node* n) {
  for (; n != nullptr; n = n->next) {
    co_await visit_one(n);
  }
}
```

**Don't:** Expect HALO to optimize recursive coroutine calls.

```cpp
ss::future<> visit(node* n) {
  if (n == nullptr) {
    co_return;
  }

  co_await visit_one(n);
  co_await visit(n->next);
}
```


## Yield in CPU heavy loops

**Do:** Use `coroutine::maybe_yield` to remain reactor friendly.

```cpp
for (const auto& item : items) {
  process(item);
  co_await ss::coroutine::maybe_yield();
}
```

**Don't:** Run an unbounded loop without a scheduling point.

```cpp
for (const auto& item : unbounded_input) {
  process(item);
}
```

Note that this is often the preferred way if it allows to keep (leaf) functions coroutine free (sync code is still the fastest).

## References

- [Coroutines Optimization Opportunities from Compiler's perspective](https://chuanqixu9.github.io/c++/2026/03/27/C++20-Coroutines-from-compiler-and-library-authors-perspective.en.html)
- [Clang `coro_await_elidable`](https://clang.llvm.org/docs/AttributeReference.html#coro-await-elidable)
- [Clang `coro_await_elidable_argument`](https://clang.llvm.org/docs/AttributeReference.html#coro-await-elidable-argument)
- [Clang `coro_only_destroy_when_complete`](https://clang.llvm.org/docs/AttributeReference.html#coro-only-destroy-when-complete)
