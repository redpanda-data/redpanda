print(jit.version)
print("hb_fail_wal_segment_append")
-- return a tuple of
-- 1) Wether or not to inject a failure
-- 2) Error code
-- 3) Error category
-- 4) Explanation
function honey_badger_fn (filename, line, cpp_module, cpp_func)
  is_exception = false
  error_code = 66
  category = cpp_module
  msg = cpp_func .. ": Inject Permanent Failure"
  if cpp_func == "wal_segment::append" then
    is_exception = true
  end
  return is_exception, error_code, category, msg
end
