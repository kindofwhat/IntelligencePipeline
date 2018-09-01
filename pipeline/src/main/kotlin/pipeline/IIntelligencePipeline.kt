package pipeline

import pipeline.capabilities.CapabilityRegistry
import pipeline.capabilities.DefaultCapabilityRegistry


interface IIntelligencePipeline {
    fun all():List<datatypes.DataRecord>
    fun registerChunkMetadataProducer(producer: participants.ChunkMetadataProducer)
    fun registerChunkProducer(name: kotlin.String, chunkProducer: participants.ChunkProducer)
    fun registerSideEffect(name: kotlin.String, sideEffect: participants.PipelineSideEffect)
    fun registerMetadataProducer(prod: participants.MetadataProducer)
    fun <I,U>registerProposer(prod: facts.Proposer<I,U>)
    fun registerDocumentRepresentationProducer(prod: participants.DocumentRepresentationProducer)
    fun registerIngestor(ingestor: participants.PipelineIngestor)
    fun stop()
    fun run()

    val registry:DefaultCapabilityRegistry
}