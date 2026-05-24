(module
  (memory (export "memory") 2)
  (global $heap (mut i32) (i32.const 1024))
  (global $out_len (mut i32) (i32.const 0))
  (global $out_ptr (mut i32) (i32.const 65536))

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
    global.get $out_len)

  (func (export "init") (param i32 i32) (result i32)
    i32.const 0)

  (func (export "shutdown") (result i32)
    i32.const 0)

  (func (export "transform") (param $ptr i32) (param $len i32) (result i32)
    (local $i i32)
    (block $done
      (loop $copy
        local.get $i
        local.get $len
        i32.ge_u
        br_if $done

        global.get $out_ptr
        local.get $i
        i32.add
        local.get $ptr
        local.get $i
        i32.add
        i32.load8_u
        i32.store8

        local.get $i
        i32.const 1
        i32.add
        local.set $i

        br $copy))

    local.get $len
    global.set $out_len
    global.get $out_ptr)
)
