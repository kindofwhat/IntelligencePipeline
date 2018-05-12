package unittests

import facts.MaxEvaluator
import facts.Proposition
import facts.PropositionEvaluation
import org.junit.Test

class FactsTest {
    class TestProposition(proposal: Int, confidence: Float) : Proposition<Int>(proposal, confidence)

    @Test
    fun testMaxEvaluator() {

        val evaluator = MaxEvaluator<Int>()

        val res = evaluator.evaluatePropositions(setOf(
                TestProposition(12, confidence = 0.6f),
                TestProposition(1, confidence = 0.1f),
                TestProposition(14, confidence = 0.6f)))
        assert(res.size==1)
    }

}