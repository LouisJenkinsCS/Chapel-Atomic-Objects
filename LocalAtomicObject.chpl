extern {
  inline int cas128bit(void *srcvp, void *cmpvp, void *withvp) {
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

  inline void write128bit(void *srcvp, void *valvp) {
    uint128_t *src = srcvp;
    uint128_t with_val = *(uint128_t *)valvp;
    uint128_t cmp_val = *src;
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

  inline void read128bit(void *srcvp, void *dstvp) {
    uint128_t *src = srcvp;
    uint128_t with_val = *src;
    uint128_t cmp_val;
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

record ABAProtection {
  type objType;
  var ptr : uint(64);
  var cnt : uint(64);

  proc init(type objType, ptr : uint(64)) {
    this.objType = objType;
    this.ptr = ptr;
  }

  proc init(type objType) {
    this.objType = objType;
  }

  proc _ABAProtection_value {
    return __primitive("cast", objType, ptr);
  }

  forwarding _ABAProtection_value;
}

record LocalAtomicObject {
  type objType;
  var _atomicVar: atomic uint(64);
  param doABAProtection : bool;
  var _abaVar : ABAProtection;

  proc init(type objType, param doABAProtection = false) {
    this.objType = objType;
    this.doABAProtection = doABAProtection;
  }

  inline getAddrAndLocality(obj : objType) : (locale, uint(64)) {
    return (obj.locale, getAddr(obj));
  }

  inline proc getAddr(obj : objType) : uint(64) {
    return __primitive("cast", uint(64), __primitive("_wide_get_addr", obj));
  }

  inline proc read() {
    if doABAProtection {
      var dest : ABAProtection(objType);
      read128bit(c_ptrTo(_abaVar), c_ptrTo(dest));
      return dest;
    } else {
      return __primitive("cast", objType, _atomicVar.read());
    }
  }

  inline proc compareExchange(expectedObj:objType, newObj:objType) {
    if boundsChecking && (expectedObj.locale != this.locale || newObj.locale != this.locale) then
        halt("Attempt to compare ", getAddrAndLocality(expectedObj), " and exchange ", getAddrAndLocality(newObj),
            " when expected to be hosted on ", this.locale);
    if doABAProtection {
      var cmp : ABAProtection(objType, getAddr(expectedObj));
      cas128bit(c_ptrTo(_abaVar), c_ptrTo())
    }
    return _atomicVar.compareExchangeStrong(getAddr(expectedObj), getAddr(newObj));
  }

  inline proc write(newObj:objType) {
    if boundsChecking && newObj.locale == this.locale then
      _atomicVar.write(getAddr(newObj));
  }

  inline proc exchange(newObj:objType) {
    if boundsChecking then
      if __primitive("is wide pointer", newObj) then
        halt("Attempt to exchange a wide pointer into LocalAtomicObject");

    const curObj = _atomicVar.exchange(getAddr(newObj));
    return __primitive("cast", objType, curObj);
  }

  // handle wrong types
  inline proc write(newObj) {
    compilerError("Incompatible object type in LocalAtomicObject.write: ",
        newObj.type);
  }

  inline proc compareExchange(expectedObj, newObj) {
    compilerError("Incompatible object type in LocalAtomicObject.compareExchange: (",
        expectedObj.type, ",", newObj.type, ")");
  }

  inline proc exchange(newObj) {
    compilerError("Incompatible object type in LocalAtomicObject.exchange: ",
        newObj.type);
  }
}
