package datatypes
/*
 * things to consider:
 * - tagging/taxonomies
 * - named entity recognition
 * - sentiment analysis
 * - summarization
 * - transl8ion
 * - keyword extraction
 *
 * intermediate formats
 *  - tokenized
 *  - stemmed
 *  - text extracted (OK: DocumentRepresentation)

 */

import kotlinx.serialization.Serializable

enum class EntityType {
    PERSON, LOCATION, DATE, TIME, SUM, GENERIC,
}

@Serializable data class NamedEntity(val type: String  ="", val value:String ="")


@Serializable data class NamedEntityRelation(val entity:NamedEntity = NamedEntity(), val container:TextContainer=Chunk())