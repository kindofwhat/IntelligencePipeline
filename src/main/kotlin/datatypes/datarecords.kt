package datatypes

import kotlinx.serialization.Serializable
import participants.NoOpIngestor
import participants.PipelineIngestor


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
                                  var createdBy: String=NoOpIngestor().name)

@Serializable
data class DataRecord(val name: String="",
                      val representation: DocumentRepresentation= DocumentRepresentation(),
                      val additionalRepresentations: Set<DocumentRepresentation> = mutableSetOf(),
                      val meta: Set<Metadata> = mutableSetOf())
