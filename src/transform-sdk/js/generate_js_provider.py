#!/usr/bin/env python3

import sys

WAT_TEMPLATE = """
(module
	(memory (import "js_vm" "memory") 0)

	(func (export "file_length") (result i32)
          (i32.const {length})
        )

	(func (export "get_file") (param $buffer_ptr i32)
		;; Copy the bytecode from data segment 0.
		(memory.init $bytecode
			(local.get $buffer_ptr)       ;; Memory destination
			(i32.const 0)                 ;; Start index
			(i32.const {length})          ;; Length
                )
		(data.drop $bytecode)
        )
	(data $bytecode "\\{bytecode}"))
"""

source = sys.stdin.read()
sys.stdout.write(
    WAT_TEMPLATE.format(length=len(source),
                        bytecode=source.encode().hex('\\')))
