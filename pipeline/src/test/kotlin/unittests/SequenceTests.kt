package unittests

import org.junit.Test
import chunks.*

class SequenceTests {

    @Test
    fun testAppend() {
        assert(append(1,emptySequence<Int>()).toList() == listOf(1))
        assert(append(1, sequenceOf(1,2,3)).toList() == listOf(1,2,3,1))
    }

    @Test
    fun testRemove() {
        assert(remove(1, sequenceOf(1,2,3)).toList() == listOf(1,3))
        assert(remove(1, emptySequence<Int>())== emptySequence<Int>())
    }

    @Test
    fun testInsert() {
        assert(insert(1, 1, sequenceOf(1,2,3)).toList() == listOf(1,1,2,3))
        assert(insert(1,0, emptySequence<Int>()).toList()== listOf(1))
        assert(insert(1,0, emptySequence<Int>()).toList()== listOf(1))
        //ok?
        assert(insert(1,2, emptySequence<Int>()).toList()== listOf(1))
    }

    @Test
    fun testUpdate() {
        assert(update(1, 2, sequenceOf(1,2,3)).toList() == listOf(1,2,1))
        assert(update(1,0, emptySequence<Int>())==emptySequence<Int>())
        assert(update(1,2, emptySequence<Int>())==emptySequence<Int>())
    }

    @Test
    fun testMerge() {
        assert(merge(4, 1, 2, sequenceOf(1,2,3)).toList() == listOf(1,4))
        assert(merge(4, 0, 2, sequenceOf(1,2,3)).toList() == listOf(4))
        assert(merge(1,0,2, emptySequence<Int>())==emptySequence<Int>())
    }

}