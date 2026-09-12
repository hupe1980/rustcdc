;; ABI v2 pass-through transform fixture *with a data segment*.
;;
;; Identical to `pass_through.wat` except for the `(data ...)` directive. That one
;; line is the entire point of this fixture: wasmtime checks the store's epoch
;; deadline while initialising data segments, so a host that arms the deadline
;; after `instantiate` rejects this module — and every real module, since Rust,
;; AssemblyScript and TinyGo all emit a data segment for string literals and
;; rodata — while a data-segment-free WAT suite stays green.
;;
;; The segment is placed at 65536 (start of the second page) so it can never
;; collide with the bump allocator, which grows upward from offset 8.
(module
  (memory (export "memory") 2)

  (data (i32.const 65536) "rustcdc-data-segment-fixture")

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

  ;; Reset the heap when the base allocation is freed; see `pass_through.wat`.
  (func (export "dealloc") (param $ptr i32) (param $size i32)
    local.get $ptr
    i32.const 8
    i32.eq
    if
      i32.const 8
      global.set $heap
    end)

  (func (export "rustcdc_abi_version") (result i32)
    i32.const 2)

  ;; Copy $len bytes from $src to $dst byte-by-byte.
  (func $memcpy (param $dst i32) (param $src i32) (param $len i32)
    (local $i i32)
    (local.set $i (i32.const 0))
    (block $done
      (loop $loop
        (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
        (i32.store8
          (i32.add (local.get $dst) (local.get $i))
          (i32.load8_u (i32.add (local.get $src) (local.get $i))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $loop))))

  ;; Copy the input to a fresh allocation and return (ptr << 32) | len.
  (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
    (local $out i32)
    (local.set $out (call $alloc (local.get $len)))
    (call $memcpy (local.get $out) (local.get $ptr) (local.get $len))
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
      (i64.extend_i32_u (local.get $len)))))
