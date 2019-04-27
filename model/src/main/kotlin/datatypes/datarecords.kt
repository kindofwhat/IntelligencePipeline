package datatypes

import facts.Proposition
import kotlinx.serialization.Optional
import kotlinx.serialization.Serializable


@Serializable
data class Chunk(val type: String = "GENERAL", val command: String = "ADD",
                 val index: Long = -1, val parent: DataRecord =DataRecord(), val content: String = "",
                 @Optional val meta: Set<Metadata> = mutableSetOf()) : TextContainer {
    override fun id(): String {
        return "${parent.id()}-$type-$command-$index"
    }
}

/**
 * describes a meta datum, which has been created by e.g. a parser of a file
 */
@Serializable
data class Metadata(val values: Map<String, String> = mutableMapOf(), val createdBy: String = "", val container: TextContainer=DataRecord())

/**
 * describes a representation of a document
 */
@Serializable
data class DocumentRepresentation(val path: String = "",
                                  var createdBy: String = "none")


@Serializable
interface TextContainer {
   fun id():String
}

/**
 * this is our main domain object.
 *
 */
@Serializable
data class DataRecord(val name: String = "",
                      val timestamp: Long = 0L,
                      @Optional val representation: DocumentRepresentation = DocumentRepresentation(),
                      @Optional val additionalRepresentations: Set<DocumentRepresentation> = mutableSetOf()) : TextContainer {
    override fun id(): String {
        return name
    }
}

