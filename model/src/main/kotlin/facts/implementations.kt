package facts

/**
 * returns first max value with a confidence of 1.0
 */
class MaxScore<U:Comparable<U>>: Evaluator<U> {
    val name="max-evaluator"
    override fun evaluatePropositions(propositions: Set<Proposition<U>>): PropositionScore<U> {
        val max = propositions
                .maxBy { proposition -> proposition.proposal }
        if(max != null) return PropositionScore(max, 1.0f, name)
        else return PropositionScore(null, 0.0f, name)
    }
}

/**
 * returns empty evaluation
 */
class NoScore<U:Comparable<U>>: Evaluator<U> {
    val name="max-evaluator"
    override fun evaluatePropositions(propositions: Set<Proposition<U>>): PropositionScore<U> {
        return PropositionScore(null, 0.0f, name)
    }
}