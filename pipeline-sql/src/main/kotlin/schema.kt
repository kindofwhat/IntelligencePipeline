import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.LongEntity
import org.jetbrains.exposed.dao.LongEntityClass
import org.jetbrains.exposed.dao.LongIdTable
import org.jetbrains.exposed.sql.*
/*
@Serializable
data class Chunk(val type: String = "GENERAL", val command: String = "ADD",
                 val index: Long = -1, val parent: DataRecord =DataRecord(), val content: String = "",
                 @Optional val meta: Set<Metadata> = mutableSetOf()) : TextContainer {
    override fun id(): String {
        return "${parent.id()}-$type-$command-$index"
    }
}
 */
/*
@Serializable
data class Metadata(val values: Map<String, String> = mutableMapOf(), val createdBy: String = "", val container: TextContainer=DataRecord())

 */

/*
data class DocumentRepresentation(val path: String = "",
                                  var createdBy: String = "none") */


object DocumentRepresentations: LongIdTable() {
    val path = varchar("name", length = 4096).index() // Column<String>
    val createdBy = varchar("createdBy", length = 2048) // Column<String>
}


class DocumentRepresentation(id: EntityID<Long>): LongEntity(id) {
    companion object : LongEntityClass<DocumentRepresentation>(DocumentRepresentations)

    var path by DocumentRepresentations.path
    var createdBy by DocumentRepresentations.createdBy
}
/*
@Serializable
data class DataRecord(val name: String = "",
                      val timestamp: Long = 0L,
                      @Optional val representation: DocumentRepresentation = DocumentRepresentation(),
                      @Optional val additionalRepresentations: Set<DocumentRepresentation> = mutableSetOf()) : TextContainer {
    override fun id(): String {
        return name
    }
}

 */
object DataRecords : LongIdTable() {
    val name = varchar("name", length = 2048).index() // Column<String>
    val timestamp = long("timestamp")
    val representation = reference("representation", DocumentRepresentations)

}

class DataRecord(id: EntityID<Long>): LongEntity(id) {
    companion object : LongEntityClass<DataRecord>(DataRecords)

    var name by DataRecords.name
    var timestamp by DataRecords.timestamp
    var representation by DataRecord referencedOn  DataRecords.representation
}

