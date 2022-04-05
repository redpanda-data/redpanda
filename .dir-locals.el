;; Copyright 2020 Redpanda Data, Inc.
;;
;; Use of this software is governed by the Business Source License
;; included in the file licenses/BSL.md
;;
;; As of the Change Date specified in that file, in accordance with
;; the Business Source License, use of this software will be governed
;; by the Apache License, Version 2.0

;;; Directory Local Variables
;;; For more information see (info "(emacs) Directory Variables")

((nil
  (lsp-file-watch-ignored-directories "/\\.git$" "/\\.ccls-cache$" "/build$" "/vbuild$"))
 ;; cannot be c-or-c++-mode; it must be c++ mode for M-x compile
 ;; directories must be quoted, even if M-x add-dir-local-variable
 ;; adds them without quotes
 (c++-mode
  (helm-make-build-dir . "vbuild/debug/clang")
  )
 (c-or-c++-mode
  (clang-format-executable . vbuild/llvm/install/bin/clang-format)))
