package participants

import datatypes.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.SendChannel
import pipeline.capabilities.CapabilityLookup
import pipeline.capabilities.CapabilityLookupStrategy
import pipeline.capabilities.CapabilityRegistry

/**
 * marker interface for participant
 */
interface PipelineParticipant{
    val name:String
}

/**
 * An ingestor is a source for the pipeline
 */
interface PipelineIngestor : PipelineParticipant {
    /**
     * uses the channel as a sink after creating a DocumentRepresentation
     * This channel is provided by e.g. the intelligence pipeline itself
     */
    suspend fun ingest(channel: SendChannel<DocumentRepresentation>)
}

/**
 * creates a MetaData for a DataRecord
 */
interface MetadataProducer : PipelineParticipant {
    fun metadataFor(record: DataRecord): Metadata
}

abstract class CapabilityLookupStrategyMetadataProducer<T>(val strategy: CapabilityLookupStrategy):MetadataProducer

interface DocumentRepresentationProducer : PipelineParticipant {
    fun documentRepresentationFor(record: DataRecord): DocumentRepresentation
}


