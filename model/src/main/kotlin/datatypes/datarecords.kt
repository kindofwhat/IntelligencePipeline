package datatypes

import kotlinx.serialization.Serializable
enum class ChunkCommand { START, ADD, APPEND,
    INSERT,UPDATE,MERGE, DELETE, LAST }
enum class ChunkType { GENERAL, PAGE, PARAGRAPH,SENTENCE,WORD }

@Serializable
data class Chunk(val type: ChunkType = ChunkType.GENERAL, val command: ChunkCommand = ChunkCommand.ADD,
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
                      val representation: DocumentRepresentation = DocumentRepresentation(),
                      val additionalRepresentations: Set<DocumentRepresentation> = mutableSetOf(),
                      val meta: Set<Metadata> = mutableSetOf())

interface Event<C,P>
@Serializable data class DataRecordEvent(val command: DataRecordCommand = DataRecordCommand.UPSERT, val record: DataRecord = DataRecord()): Event<BaseCommand, DataRecord>
@Serializable data class MetadataEvent(val command: BaseCommand = BaseCommand.UPSERT, val record: Metadata = Metadata()): Event<BaseCommand, Metadata>
@Serializable data class DocumentRepresentationEvent(val command: BaseCommand = BaseCommand.UPSERT, val record: DocumentRepresentation = DocumentRepresentation()): Event<BaseCommand, DocumentRepresentation>
