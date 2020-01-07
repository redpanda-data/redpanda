;;; Directory Local Variables
;;; For more information see (info "(emacs) Directory Variables")

((nil
  (lsp-file-watch-ignored "/\\.git$" "/\\.ccls-cache$" "/build$"))
 (c-or-c++-mode
  (helm-make-build-dir . build/debug/gcc)
  (clang-format-executable . build/llvm/llvm-bin/bin/clang-format)))
