package facts

import datatypes.Metadata
import kotlinx.serialization.Serializable

open class PropositionType<U>(val name:String, val result: U)
/**
 * A proposition about @val proposal with the confidence, where 0 is totally unsure and 1.0 is 100% sure
 *
 */
@Serializable
open class Proposition<U>(val proposal:U, val confidence:Float)

class MetadataProposition(proposal:Metadata):Proposition<Metadata>(proposal, confidence = 1.0f)

interface Proposer<I,U> {
    fun propose( proposedFor:I): Proposition<U>
}

/**
*   Example proposition: what language does a DataRecord have....
 */


/**
 * An "independent" evaluation of an Proposition: the score should be between 0 and 1
 */
@Serializable
data class PropositionScore<U> (val proposition: Proposition<U>?, val score:Float, val evaluatedBy:String)

/**
 * evaluates a bunch of propositions and gives them a combined, evaluated score.
 */
interface Evaluator<U> {
    fun evaluatePropositions(propositions:Set<Proposition<U>>): PropositionScore<U>
}
