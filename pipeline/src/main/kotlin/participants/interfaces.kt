package participants

import kotlinx.coroutines.experimental.channels.SendChannel
import pipeline.capabilities.CapabilityLookupStrategy

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
    suspend fun ingest(channel: SendChannel<datatypes.DocumentRepresentation>)
}

typealias PipelineSideEffect = (key:Long, value: datatypes.DataRecord) -> Unit


/**
 * this would be an alternative: use functional types
 */
typealias MetadataProducerF = (value: datatypes.DataRecord) -> datatypes.Metadata

/**
 * creates a MetaData for a DataRecord
 */
interface MetadataProducer : PipelineParticipant {
    fun metadataFor(record: datatypes.DataRecord): datatypes.Metadata
}

/**
 * creates metadata from a chunk
 */
interface ChunkMetadataProducer : PipelineParticipant {
    fun metadataFor(chunk: datatypes.Chunk): datatypes.Metadata
}

/**
 * creates a stream of "chunks" of a datarecord. Those chunks may be paragraphs, sentences, words
 */
interface ChunkProducer : PipelineParticipant {
    suspend fun chunks(record: datatypes.DataRecord, recordId:Long):Sequence<datatypes.Chunk>
}

interface DocumentRepresentationProducer : PipelineParticipant {
    fun documentRepresentationFor(record: datatypes.DataRecord): datatypes.DocumentRepresentation
}


abstract class CapabilityLookupStrategyMetadataProducer<T>(val strategy: CapabilityLookupStrategy):MetadataProducer


