(module
  (memory (export "memory") 2)
  (global $heap (mut i32) (i32.const 1024))
  (global $out_len (mut i32) (i32.const 0))

  (func (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    (local $next i32)
    (local $limit i32)
    global.get $heap
    local.set $ptr

    local.get $ptr
    local.get $len
    i32.add
    local.set $next

    memory.size
    i32.const 16
    i32.shl
    local.set $limit

    local.get $next
    local.get $limit
    i32.gt_u
    if
      i32.const 1024
      local.set $ptr
      local.get $ptr
      local.get $len
      i32.add
      local.set $next
    end

    local.get $next
    global.set $heap
    local.get $ptr)

  (func (export "output_len") (result i32)
    global.get $out_len)

  (func (export "init") (param i32 i32) (result i32)
    i32.const 0)

  (func (export "shutdown") (result i32)
    i32.const 0)

  (func (export "transform") (param $ptr i32) (param $len i32) (result i32)
    local.get $len
    global.set $out_len
    local.get $ptr)
)
