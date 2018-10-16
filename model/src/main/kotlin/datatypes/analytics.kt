package datatypes

import kotlinx.serialization.Serializable

enum class EntityType {
    PERSON, LOCATION, DATE, TIME, SUM, GENERIC,
}

@Serializable class NamedEntity(val type: EntityType, val value:String)