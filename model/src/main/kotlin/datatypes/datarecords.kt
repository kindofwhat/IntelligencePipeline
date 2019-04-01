package datatypes

import facts.Proposition
import kotlinx.serialization.Optional
import kotlinx.serialization.Serializable


@Serializable
data class Chunk(val type: String = "GENERAL", val command: String = "ADD",
                 val index:Long=-1, val parentId:Long=-1, val content:String="", val metadata: Metadata = Metadata())

enum class BaseCommand { INSERT, UPDATE, UPSERT, DELETE }

enum class DataRecordCommand {
    CREATE, UPSERT, DELETE, UPSERT_METADATA, DELETE_METADATA, UPSERT_DOCUMENT_REPRESENTATION,
    DELETE_DOCUMENT_REPRESENTATION
}


/**
 * describes a meta datum, which has been created by e.g. a parser of a file
 */
@Serializable
data class Metadata(val values:Map<String,String> = mutableMapOf(), val createdBy: String="")

/**
 * describes a representation of a document
 */
@Serializable
data class DocumentRepresentation(val path:String="",
                                  var createdBy:String= "none")

/**
 * this is our main domain object.
 */
@Serializable
data class DataRecord(val name: String="",
                      val timestamp: Long = 0L,
                      @Optional val representation: DocumentRepresentation = DocumentRepresentation(),
                      @Optional val additionalRepresentations: Set<DocumentRepresentation> = mutableSetOf(),
                      @Optional val meta: Set<Metadata> = mutableSetOf(),
                      @Optional val propositions: Set<Proposition<Any>> = mutableSetOf())

@Serializable
data class DataRecordWithChunks(val dataRecord: DataRecord= DataRecord(), val chunks: Set<Chunk> = mutableSetOf())

interface Event<C,P>
@Serializable data class DataRecordEvent(val command: DataRecordCommand = DataRecordCommand.UPSERT, val record: DataRecord = DataRecord(), val timestamp: Long = 0L): Event<BaseCommand, DataRecord>
@Serializable data class MetadataEvent(val command: BaseCommand = BaseCommand.UPSERT, val record: Metadata = Metadata()): Event<BaseCommand, Metadata>
@Serializable data class DocumentRepresentationEvent(val command: BaseCommand = BaseCommand.UPSERT, val record: DocumentRepresentation = DocumentRepresentation()): Event<BaseCommand, DocumentRepresentation>
