package util

fun log(msg: String) = println("[${Thread.currentThread().name}] $msg")

class TestIterable<T>:Iterable<T> {
    override fun iterator(): Iterator<T> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
