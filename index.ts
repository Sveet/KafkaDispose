
class foo {
  [Symbol.dispose](): void {
    console.log('disposing')
  }
}
using x = new foo()