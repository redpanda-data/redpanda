print(jit.version)
-- return a tuple of
-- 1) Wether or not to inject a failure
-- 2) Error code
-- 3) Error category
-- 4) Explanation
function honey_badger_fn (filename, line, cpp_module, cpp_func)
  is_exception = true
  error_code = 66
  category = cpp_module
  msg = cpp_func .. ": Generated Failure From Static Function"
  return is_exception, error_code, category, msg
end
