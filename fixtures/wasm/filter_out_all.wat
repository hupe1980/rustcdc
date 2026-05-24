(module
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 1024))

  (func (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    global.get $heap
    local.set $ptr
    global.get $heap
    local.get $len
    i32.add
    global.set $heap
    local.get $ptr)

  (func (export "output_len") (result i32)
    i32.const 0)

  (func (export "transform") (param i32 i32) (result i32)
    i32.const -1)
)
