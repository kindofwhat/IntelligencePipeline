package facts

import datatypes.DataRecord


/**
 * A proposition about @val proposal with the confidence, where 0 is totally unsure and 1.0 is 100% sure
 *
 */
interface Proposition<T,U> {
    val proposedFor:T
    val proposal:U
    val confidence:Float
}

/**
*   Example proposition: what language does a DataRecord have....
 */
interface LanguageProposition:Proposition<DataRecord,String>

/**
 * An "independet" evaluation of an Evaluator: the score should be between 0 and 1
 */
data class PropositionEvaluation<T,U> (val proposition: Proposition<T,U>, val score:Float, val evaluatedBy:String)

/**
 * evaluates a bunch of propositions and gives them a score
 */
interface Evaluator<T,U> {
    fun evaluatePropositions(propositions:Set<Proposition<T,U>>):Set<PropositionEvaluation<T,U>>
}

