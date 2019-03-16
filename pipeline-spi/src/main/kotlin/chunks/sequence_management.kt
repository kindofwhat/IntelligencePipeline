package chunks

import java.lang.Math.max

fun <T> append(t:T, s:  Sequence<T>): Sequence<T> {
    return s + t
}


fun <T> insert(t:T, index:Int,  s:  Sequence<T>): Sequence<T> {
    return s.take(index) + t + s.drop(index)
}

fun <T> update(t:T, index:Int,  s:  Sequence<T>): Sequence<T> {
    if(s.count()<=index) return s
    return s.take(index) + t + s.drop(index + 1)
}

fun <T> merge(t:T, from:Int, to:Int,s:Sequence<T>): Sequence<T> {
    if(from>to || s.count()<=max(from,to)) return s
    return s.take(from) + t + s.drop(to+1)
}

fun <T> remove(index:Int,  s:  Sequence<T>): Sequence<T> {
    if(s.count()<=index) return s
  return s - s.elementAt(index)
}
