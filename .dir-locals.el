;; Project-wide Emacs settings
;; We use clang-format for the whole project
;; see misc/fmt.py  - but this sets up the basic
;; environment for c++
(
 (nil . ((lsp-file-watch-ignored . ("/\\.git$"
                                    ;; temporaries
                                    "/\\.ccls-cache$"
                                    "/build$"
                                    ))
         ))
 (c++-mode (helm-make-build-dir . "build/debug"))
 )
