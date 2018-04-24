package unittests

import facts.MaxEvaluator
import facts.Proposition
import facts.PropositionEvaluation
import org.junit.Test

class FactsTest {
    data class TestProposition(override val proposedFor: String, override val proposal: Int = proposedFor.length, override val confidence: Float):
            Proposition<String,Int>,Comparable<Proposition<String,Int>> {
        override fun compareTo(other: Proposition<String, Int>): Int {
            return proposedFor.compareTo(other.proposedFor)
        }
    }

    @Test
    fun testMaxEvaluator() {

        val evaluator = MaxEvaluator<String,Int>()

        val res = evaluator.evaluatePropositions(sortedSetOf(
                TestProposition(proposedFor = "aaa", confidence = 0.6f),
                TestProposition(proposedFor = "two", confidence = 0.1f),
                TestProposition(proposedFor = "three", confidence = 0.6f)

                ))
        assert(res.size==1)
    }

}