use Time;

extern {
#include <stdint.h>
#include <stdio.h>

  struct uint128 {
    uint64_t lo;
    uint64_t hi;
  };

  typedef struct uint128 uint128_t;
  static inline int cas128bit(void *srcvp, void *cmpvp, void *withvp) {
    uint128_t *src = srcvp;
    uint128_t *cmp = cmpvp;
    uint128_t *with = withvp;
    char result;

    __asm__ __volatile__ ("lock; cmpxchg16b (%6);"
        "setz %7; "
        : "=a" (cmp->lo),
        "=d" (cmp->hi)
        : "0" (cmp->lo),
        "1" (cmp->hi),
        "b" (with->lo),
        "c" (with->hi),
        "r" (src),
        "m" (result)
        : "cc", "memory");

    return result;
  }

  static inline void write128bit(void *srcvp, void *valvp) {
    uint128_t *src = srcvp;
    uint128_t with_val = *(uint128_t *)valvp;
    uint128_t __attribute__ ((aligned (16))) cmp_val = *src;
    uint128_t *cmp = &cmp_val;
    uint128_t *with = &with_val;
    char successful = 0;

    while (!successful) {
      __asm__ __volatile__ ("lock; cmpxchg16b (%6);"
          "setz %7; "
          : "=a" (cmp->lo),
          "=d" (cmp->hi)
          : "0" (cmp->lo),
          "1" (cmp->hi),
          "b" (with->lo),
          "c" (with->hi),
          "r" (src),
          "m" (successful)
          : "cc", "memory");
    }
  }

  static inline void read128bit(void *srcvp, void *dstvp) {
    uint128_t *src = srcvp;
    uint128_t with_val = *src;
    uint128_t __attribute__ ((aligned (16))) cmp_val;
    uint128_t *cmp = &cmp_val;
    uint128_t *with = &with_val;
    char result;

    __asm__ __volatile__ ("lock; cmpxchg16b (%6);"
        "setz %7; "
        : "=a" (cmp->lo),
        "=d" (cmp->hi)
        : "0" (cmp->lo),
        "1" (cmp->hi),
        "b" (with->lo),
        "c" (with->hi),
        "r" (src),
        "m" (result)
        : "cc", "memory");

    *(uint128_t *)dstvp = cmp_val;
  }
}


class C {
  var x : int;
  var y : int;
}

config const nElements = 1000000;

proc hardwareReadTest() {
  var c = new C(1,1);
  var timer = new Timer();
  timer.start();
  coforall loc in Locales do on loc {
    coforall tid in 0 .. #here.maxTaskPar {
      for i in 1 .. nElements / here.maxTaskPar {
        on c {
          var _c = c;
          var dest : C;
          on Locales[here.id] do dest = nil;
          read128bit(c_ptrTo(_c) : c_void_ptr, c_ptrTo(dest) : c_void_ptr);
          assert(dest == c);
        }
      }
    }
  }
  timer.stop();

  writeln("Hardware Read Test(", nElements, " Elements): ", timer.elapsed());
}

proc proof_of_correctness() {
  var c1 : C;
  var c2 : C;
  on Locales[here.id] do c1 = new C(1,1);
  on Locales[here.id] do c2 = new C(2,2);
  var c3 : C = c1;

  var result = cas128bit(c_ptrTo(c1) : c_void_ptr, c_ptrTo(c3) : c_void_ptr, c_ptrTo(c2) : c_void_ptr);
  writeln("Result: ", result, ", c1: ", c1, ", c2: ", c2, ", c3: ", c3);

  var c4 : C;
  on Locales[here.id] do c4 = nil;
  read128bit(c_ptrTo(c1) : c_void_ptr, c_ptrTo(c4) : c_void_ptr);
  writeln("Atomic Result: ", c4);
  /*writeln(c1);*/
}

proc main() {
  hardwareReadTest();
}
