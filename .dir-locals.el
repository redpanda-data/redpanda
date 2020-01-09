;;; Directory Local Variables
;;; For more information see (info "(emacs) Directory Variables")

((nil
  (lsp-file-watch-ignored "/\\.git$" "/\\.ccls-cache$" "/build$"))
 ;; cannot be c-or-c++-mode; it must be c++ mode for M-x compile
 ;; directories must be quoted, even if M-x add-dir-local-variable
 ;; adds them without quotes
 (c++-mode
  (helm-make-build-dir . "build/debug/clang")
  (clang-format-executable . "build/llvm/llvm-bin/bin/clang-format")))
