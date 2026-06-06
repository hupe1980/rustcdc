;; ABI v2 pass-through transform fixture.
;;
;; Copies the input bytes to a fresh allocation and returns the packed
;; (out_ptr << 32 | out_len) result as i64. Address 0 is never used;
;; the bump allocator starts at offset 8.
(module
  (memory (export "memory") 2)

  ;; Bump allocator: starts at offset 8 to keep address 0 reserved.
  (global $heap (mut i32) (i32.const 8))

  (func $alloc (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    global.get $heap
    local.tee $ptr
    local.get $len
    i32.add
    global.set $heap
    local.get $ptr)

  ;; ABI v2: dealloc is a no-op for bump allocator (no free list).
  (func (export "dealloc") (param i32) (param i32))

  (func (export "rustcdc_abi_version") (result i32)
    i32.const 2)

  ;; Copy $len bytes from $src to $dst byte-by-byte.
  (func $memcpy (param $dst i32) (param $src i32) (param $len i32)
    (local $i i32)
    i32.const 0
    local.set $i
    (block $brk
      (loop $lp
        local.get $i
        local.get $len
        i32.ge_u
        br_if $brk
        local.get $dst
        local.get $i
        i32.add
        local.get $src
        local.get $i
        i32.add
        i32.load8_u
        i32.store8
        local.get $i
        i32.const 1
        i32.add
        local.set $i
        br $lp)))

  ;; Pass-through: allocate output buffer, copy input into it, return packed result.
  (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
    (local $out_ptr i32)
    local.get $len
    call $alloc
    local.tee $out_ptr
    local.get $ptr
    local.get $len
    call $memcpy
    local.get $out_ptr
    i64.extend_i32_u
    i64.const 32
    i64.shl
    local.get $len
    i64.extend_i32_u
    i64.or)

  (func (export "init") (param i32 i32) (result i32)
    i32.const 0)

  (func (export "shutdown") (result i32)
    i32.const 0)
)
