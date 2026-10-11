; The recursion checker's fixture (scripts/check_recursion.py --self-test): IR shaped as rustc
; emits it at opt-level 0, its symbols v0-mangled.

; focal_fixture::walk calls itself.
define void @_RNvCs1234_13focal_fixture4walk(i64 %depth) unnamed_addr {
start:
  call void @_RNvCs1234_13focal_fixture4walk(i64 %depth)
  ret void
}

; focal_fixture::even and focal_fixture::odd call each other.
define zeroext i1 @_RNvCs1234_13focal_fixture4even(i64 %n) unnamed_addr {
start:
  %r = call zeroext i1 @_RNvCs1234_13focal_fixture3odd(i64 %n)
  ret i1 %r
}

define zeroext i1 @_RNvCs1234_13focal_fixture3odd(i64 %n) unnamed_addr personality ptr @rust_eh_personality {
start:
  %r = invoke zeroext i1 @_RNvCs1234_13focal_fixture4even(i64 %n)
          to label %done unwind label %cleanup
done:
  ret i1 %r
cleanup:
  %lp = landingpad { ptr, i32 } cleanup
  resume { ptr, i32 } %lp
}

; The drop glue of a self-referential type: Node holds a Box<Node>.
define void @_RINvNtCs5678_4core3ptr13drop_in_placeNtCs1234_13focal_fixture4NodeE(ptr %self) unnamed_addr {
start:
  call void @_RINvNtCs5678_4core3ptr13drop_in_placeINtNtCs9abc_5alloc5boxed3BoxNtCs1234_13focal_fixture4NodeEE(ptr %self)
  ret void
}

define void @_RINvNtCs5678_4core3ptr13drop_in_placeINtNtCs9abc_5alloc5boxed3BoxNtCs1234_13focal_fixture4NodeEE(ptr %self) unnamed_addr {
start:
  call void @_RINvNtCs5678_4core3ptr13drop_in_placeNtCs1234_13focal_fixture4NodeE(ptr %self)
  call void @llvm.memcpy.p0.p0.i64(ptr %self, ptr %self, i64 8, i1 false)
  ret void
}

; focal_fixture::iterate loops and calls walk once: not a cycle of its own.
define void @_RNvCs1234_13focal_fixture7iterate(i64 %n) unnamed_addr {
start:
  br label %loop
loop:
  call void @_RNvCs1234_13focal_fixture4walk(i64 %n)
  br label %loop
}

; A dependency's own recursion: listed, not refused.
define void @_RNvCs9999_13serde_fixture4nest(i64 %depth) unnamed_addr {
start:
  tail call void @_RNvCs9999_13serde_fixture4nest(i64 %depth)
  ret void
}

; The drop glue as rustc 1.98 names it, `drop_glue`, of a type holding a Box of itself. Symbols
; already demangled pass the demangler unchanged.
define void @"core::ptr::drop_glue::<focal_fixture::Tree>"(ptr %self) unnamed_addr {
start:
  call void @"core::ptr::drop_glue::<alloc::boxed::Box<focal_fixture::Tree>>"(ptr %self)
  ret void
}

define void @"core::ptr::drop_glue::<alloc::boxed::Box<focal_fixture::Tree>>"(ptr %self) unnamed_addr {
start:
  call void @"core::ptr::drop_glue::<focal_fixture::Tree>"(ptr %self)
  ret void
}

; A channel's teardown whose message holds the receiving end of its own channel: std's
; functions alone, one of ours their message (hyper-block's simulated device, 2026-10-10).
define void @"<std::sync::mpmc::Receiver<focal_fixture::Reply> as core::ops::drop::Drop>::drop"(ptr %self) unnamed_addr {
start:
  call void @"std::sync::mpmc::counter::Receiver<std::sync::mpmc::list::Channel<focal_fixture::Reply>>::release"(ptr %self)
  ret void
}

define void @"std::sync::mpmc::counter::Receiver<std::sync::mpmc::list::Channel<focal_fixture::Reply>>::release"(ptr %self) unnamed_addr {
start:
  call void @"std::sync::mpmc::list::Channel<focal_fixture::Reply>::discard_all_messages"(ptr %self)
  ret void
}

define void @"std::sync::mpmc::list::Channel<focal_fixture::Reply>::discard_all_messages"(ptr %self) unnamed_addr {
start:
  call void @"<std::sync::mpmc::Receiver<focal_fixture::Reply> as core::ops::drop::Drop>::drop"(ptr %self)
  ret void
}

; The same teardown with a dependency's message: listed, not refused.
define void @"<std::sync::mpmc::Receiver<serde_fixture::Message> as core::ops::drop::Drop>::drop"(ptr %self) unnamed_addr {
start:
  call void @"std::sync::mpmc::list::Channel<serde_fixture::Message>::discard_all_messages"(ptr %self)
  ret void
}

define void @"std::sync::mpmc::list::Channel<serde_fixture::Message>::discard_all_messages"(ptr %self) unnamed_addr {
start:
  call void @"<std::sync::mpmc::Receiver<serde_fixture::Message> as core::ops::drop::Drop>::drop"(ptr %self)
  ret void
}

declare void @llvm.memcpy.p0.p0.i64(ptr, ptr, i64, i1)
declare i32 @rust_eh_personality(...)
