package facts

/**
 * returns first max value with a confidence of 1.0
 */
class MaxEvaluator<U:Comparable<U>>:Evaluator<U> {
    val name="max-evaluator"
    override fun evaluatePropositions(propositions: Set<Proposition<U>>): Set<PropositionEvaluation<U>> {
        val max = propositions
                .maxBy { proposition -> proposition.proposal }
        if(max != null) return setOf(PropositionEvaluation(max,1.0f,name))
        else return emptySet()
    }
}