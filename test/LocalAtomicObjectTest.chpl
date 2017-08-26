use LocalAtomicObject;

class Foo {
  var x = 10;

  proc print() {
    writeln("Foo.x = ", x);
  }
}

proc main() {
  var tail: LocalAtomicObject(Foo);
  var initNode = new Foo(x=20);
  var newNode = new Foo(x=30);

  tail.write(initNode);
  tail.read().print();
  var oldNode = tail.exchange(newNode);
  oldNode.print();
  tail.read().print();
}
