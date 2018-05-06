package facts

import datatypes.DataRecord


/**
 * A proposition about @val proposal with the confidence, where 0 is totally unsure and 1.0 is 100% sure
 *
 */
open class Proposition<U>(val proposal:U, val confidence:Float)

interface Proposer<T,U> {
    fun propose( proposedFor:T): Proposition<U>
}

/**
*   Example proposition: what language does a DataRecord have....
 */


/**
 * An "independent" evaluation of an Proposition: the score should be between 0 and 1
 */
data class PropositionEvaluation<U> (val proposition: Proposition<U>, val score:Float, val evaluatedBy:String)

/**
 * evaluates a bunch of propositions and gives them a score
 */
interface Evaluator<U> {
    fun evaluatePropositions(propositions:Set<Proposition<U>>):Set<PropositionEvaluation<U>>
}
