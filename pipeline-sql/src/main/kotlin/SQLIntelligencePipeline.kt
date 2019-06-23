import datatypes.*
import kotlinx.coroutines.channels.ReceiveChannel
import org.jetbrains.exposed.sql.Database
import orientdb.ChannelIntelligencePipeline

class SQLIntelligencePipeline: ChannelIntelligencePipeline() {

    init {
        Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
    }
    override fun loadDataRecord(value: DataRecord): DataRecord? {

        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun saveDataRecord(value: DataRecord): DataRecord? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun loadMetadata(value: Metadata): Metadata? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun saveMetadata(value: Metadata): Metadata? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun loadChunk(value: Chunk): Chunk? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun saveChunk(value: Chunk): Chunk? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun saveNamedEntity(chunk: Chunk, ne: NamedEntity): NamedEntityRelation? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun dataRecords(id: String): ReceiveChannel<DataRecord> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}