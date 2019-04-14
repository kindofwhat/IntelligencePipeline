package participants

import datatypes.Chunk
import datatypes.DataRecord
import kotlinx.coroutines.channels.SendChannel
import org.bouncycastle.asn1.cms.MetaData
import pipeline.capabilities.CapabilityLookupStrategy

/**
 * marker interface for participant
 */
interface PipelineParticipant<T,U>{
    val name:String
    suspend fun produce(record:T):U?
}

/**
 * An ingestor is a source for the pipeline
 */
interface PipelineIngestor  {
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
typealias MetadataProducerF = (value: datatypes.DataRecord) -> datatypes.Metadata?

/**
 * creates a MetaData for a DataRecord
 */
interface MetadataProducer : PipelineParticipant<DataRecord, datatypes.Metadata> {
    suspend override fun produce(record: datatypes.DataRecord): datatypes.Metadata?
}

/**
 * creates metadata from a chunk
 */
interface ChunkMetadataProducer : PipelineParticipant<Chunk,datatypes.Metadata> {
    suspend override fun produce(record: datatypes.Chunk): datatypes.Metadata?
}

/**
 * creates a stream of "chunks" of a datarecord. Those chunks may be pages, paragraphs, sentences, words
 */
interface ChunkProducer : PipelineParticipant<DataRecord, Sequence<datatypes.Chunk>> {
    suspend override fun produce(record: datatypes.DataRecord):Sequence<datatypes.Chunk>?
}

interface DocumentRepresentationProducer : PipelineParticipant<DataRecord,datatypes.DocumentRepresentation> {
    suspend override fun produce(record: datatypes.DataRecord): datatypes.DocumentRepresentation?
}


abstract class CapabilityLookupStrategyMetadataProducer<T>(val strategy: CapabilityLookupStrategy):MetadataProducer


