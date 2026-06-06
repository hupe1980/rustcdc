;; ABI v2 filter-all transform fixture.
;;
;; Returns 0 unconditionally to signal that every event should be dropped.
(module
  (memory (export "memory") 1)

  ;; Bump allocator starting at offset 8 (address 0 is reserved).
  (global $heap (mut i32) (i32.const 8))

  (func (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    global.get $heap
    local.tee $ptr
    local.get $len
    i32.add
    global.set $heap
    local.get $ptr)

  ;; ABI v2: dealloc is a no-op for bump allocator.
  (func (export "dealloc") (param i32) (param i32))

  (func (export "rustcdc_abi_version") (result i32)
    i32.const 2)

  ;; Return 0 = drop the event.
  (func (export "transform") (param i32) (param i32) (result i64)
    i64.const 0)
)
