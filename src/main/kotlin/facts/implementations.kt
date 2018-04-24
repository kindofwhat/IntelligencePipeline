package facts

/**
 * returns first max value with a confidence of 1.0
 */
class MaxEvaluator<T,V:Comparable<V>>:Evaluator<T,V> {
    val name="max-evaluator"
    override fun evaluatePropositions(propositions: Set<Proposition<T, V>>): Set<PropositionEvaluation<T, V>> {
        val max = propositions
                .maxBy { proposition -> proposition.proposal }
        if(max != null) return setOf(PropositionEvaluation(max,1.0f,name))
        else return emptySet()
    }
}