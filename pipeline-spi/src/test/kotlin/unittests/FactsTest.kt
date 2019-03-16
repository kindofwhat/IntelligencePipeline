package unittests

import facts.MaxScore
import facts.Proposition
import org.junit.Test
import java.io.File

class FactsTest {
    class TestProposition(proposal: Int, confidence: Float) : Proposition<Int>(proposal, confidence)

    @Test
    fun testMaxEvaluator() {

        val evaluator = MaxScore<Int>()

        val res = evaluator.evaluatePropositions(setOf(
                TestProposition(12, confidence = 0.6f),
                TestProposition(1, confidence = 0.1f),
                TestProposition(14, confidence = 0.6f)))
        assert(res.score == 1.0f)
    }

    @Test
    fun testCurrentDir() {
        println("currentDir: " + File(".").absolutePath)
    }
}