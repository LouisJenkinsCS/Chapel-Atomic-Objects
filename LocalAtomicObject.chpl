record LocalAtomicObject {
  type objType;
  type atomicType = uint(64);
  var _atomicVar: atomic atomicType;

  proc LocalAtomicObject(type objType) {
    _atomicVar.write(0);
  }

  inline proc getAddr(obj : objType) : atomicType {
    if __primitive("is wide pointer", obj) then return __primitive("cast", atomicType, __primitive("_wide_get_addr", obj));
    else return __primitive("cast", atomicType, obj);
  }

  inline proc read() {
    return __primitive("cast", objType, _atomicVar.read());
  }

  inline proc compareExchange(expectedObj:objType, newObj:objType) {
    if boundsChecking then
      if __primitive("is wide pointer", newObj) || __primitive("is wide pointer", expectedObj) then
        halt("Attempt to write a wide pointer into LocalAtomicObject");

    return _atomicVar.compareExchangeStrong(getAddr(expectedObj), getAddr(newObj));
  }

  inline proc write(newObj:objType) {
    if boundsChecking then
      if __primitive("is wide pointer", newObj) then
        halt("Attempt to write a wide pointer into LocalAtomicObject");
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