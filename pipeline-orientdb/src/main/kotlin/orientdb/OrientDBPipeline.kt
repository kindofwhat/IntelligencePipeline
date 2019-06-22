package orientdb

import com.orientechnologies.common.exception.OException
import com.orientechnologies.orient.core.db.*
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.executor.OResult
import com.orientechnologies.orient.core.sql.executor.OResultSet
import datatypes.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import kotlinx.serialization.ImplicitReflectionSerializer
import pipeline.capabilities.DefaultCapabilityRegistry
import util.log


class NullLoader : Loader<Any> {
    override fun invoke(session: ODatabaseSession, p2: Any): ODocument? {
        return null
    }

}


@ExperimentalCoroutinesApi
@UseExperimental(ImplicitReflectionSerializer::class)
class OrientDBPipeline(connection: String, val dbName: String, val user: String, val password: String) : ChannelIntelligencePipeline() {

    val orient: OrientDB
    val korient: KOrient
    override val registry = DefaultCapabilityRegistry()
    private val pool: ODatabasePool

    init {

        orient = OrientDB(connection, user, password, OrientDBConfig.defaultConfig())
        pool = ODatabasePool(orient, dbName, user, password)
        val loaders = mutableMapOf<String, Loader<Any>>()
        loaders.put(DataRecord::class.simpleName ?: "", FieldLoader("name"))
        //loaders.put(DocumentRepresentation::class.simpleName ?: "", FieldLoader("path"))
        //loaders.put(Metadata::class.simpleName?:"", FieldLoader("path")
        loaders.put(TextContainer::class.simpleName ?: "", NullLoader())
        loaders.put(NamedEntity::class.simpleName ?: "") { session, ne ->
            val namedEntity = ne as NamedEntity
            var res: ODocument? = null
            session.activateOnCurrentThread()
            session.reload()
            val result = session.query("SELECT FROM NamedEntity WHERE type = ?  " +
                    "and value = ?", namedEntity.type, namedEntity.value)
            if (result.hasNext()) {
                val ores = result.next()
                val existingDocument = ores.toElement() as ODocument
                result.close()
                res = existingDocument
            }
            res
        }
        loaders.put(Chunk::class.simpleName ?: "") { session, c ->
            val chunk = c as Chunk
            var res: ODocument? = null
            session.activateOnCurrentThread()
            session.reload()
            val result = session.query("SELECT FROM Chunk WHERE index = ? " +
                    "and parent.name = ? and type= ?", chunk.index, chunk.parent.name, chunk.type)
            if (result.hasNext()) {
                val ores = result.next()
                val existingDocument = ores.toElement() as ODocument
                result.close()
                res = existingDocument
            }
            res
        }
        loaders.put(NamedEntityRelation::class.simpleName ?: "") { session, ner ->
            val relation = ner as NamedEntityRelation
            var res: ODocument? = null
            session.activateOnCurrentThread()
            session.reload()
            var result: OResultSet? = null
            val container = relation.container

            session.activateOnCurrentThread()
            session.reload()
            when (container) {
                is Chunk -> result = session.query("SELECT FROM NamedEntityRelation WHERE " +
                        " entity.value = ?" +
                        "and entity.type = ?" +
                        "and container.parent.name = ? " +
                        "and container.type = ? " +
                        "and container.index = ?", ner.entity.value, ner.entity.type, container.parent.name,
                        container.type, container.index)
            }
            if (result?.hasNext() ?: false) {
                val ores = result?.next()
                val existingDocument = ores?.toElement() as ODocument
                result?.close()
                res = existingDocument
            }

            res
        }
        loaders.put(Metadata::class.simpleName ?: "") { session, m ->
            val metadata = m as Metadata
            var res: ODocument? = null
            val container = metadata.container

            var result: OResultSet? = null
            session.activateOnCurrentThread()
            session.reload()
            when (container) {
                is DataRecord -> result = session.query("SELECT FROM Metadata WHERE createdBy = ?  " +
                        "and container.name = ?", metadata.createdBy, container.name)
                is Chunk -> result = session.query("SELECT FROM Metadata WHERE createdBy = ?  " +
                        "and container.parent.name = ? " +
                        "and container.type = ? " +
                        "and container.index = ?", metadata.createdBy, container.parent.name, container.type, container.index)
            }
            if (result?.hasNext()?:false) {
                val ores = result?.next()
                val existingDocument = ores?.toElement() as ODocument
                result?.close()
                res = existingDocument
            }

            res
        }


        korient = KOrient(connection, dbName, user, password, loaders)
        orient.createIfNotExists(dbName, ODatabaseType.MEMORY)
        korient.createSchema(DataRecord::class)
        korient.createSchema(Chunk::class)
        korient.createSchema(Metadata::class)
        korient.createSchema(NamedEntity::class)
        korient.createSchema(NamedEntityRelation::class)

    }


    override fun loadDataRecord(value: DataRecord): DataRecord? {
        return korient.load(value)
    }

    override fun saveDataRecord(value: DataRecord): DataRecord? {
        return korient.save(value)
    }

    override fun loadMetadata(value: Metadata): Metadata? {
        return korient.load(value)
    }

    override fun saveMetadata(value: Metadata): Metadata? {
        return korient.save(value)
    }

    override fun loadChunk(value: Chunk): Chunk? {
        return korient.save(value)
    }

    override fun saveChunk(value: Chunk): Chunk? {
        return korient.save(value)
    }

    override fun saveNamedEntity(chunk: Chunk, ne: NamedEntity): NamedEntityRelation? {
        return korient.save(NamedEntityRelation(ne, chunk))
    }

    /**
     * watch the pipeline "live"
     */
    fun liveSubscriber(consumer: RecordConsumer) {
        pool.acquire().live("LIVE SELECT FROM ${DataRecord::class.simpleName}", DataRecordsLiveQueryListener(korient, consumer))
    }


    override fun dataRecords(id: String): ReceiveChannel<DataRecord> {
        val channel = Channel<DataRecord>()

        val session = orient.open(dbName, user, password)
        session.activateOnCurrentThread()
        val res = session.query("SELECT FROM ${DataRecord::class.simpleName}")

        while (res.hasNext()) {
            val maybeDataRecord = korient.resultToObject(res.next(), DataRecord::class)
            if (maybeDataRecord != null) {
                launch {
                    channel.send(maybeDataRecord)
                }
            }
        }
        return channel
    }


    private class DataRecordsLiveQueryListener(val korient: KOrient,
                                               val consumer: RecordConsumer) : OLiveQueryResultListener {
        override fun onError(database: ODatabaseDocument?, exception: OException?) {
            log("Exception during live query ${exception}")
        }


        override fun onCreate(database: ODatabaseDocument?, data: OResult?) {
            handleDataRecord(data)
        }

        private fun handleDataRecord(data: OResult?) {
            val dataRecord = korient.resultToObject(data, DataRecord::class)

            if (dataRecord != null) {
                consumer.invoke(dataRecord)
            }

        }

        override fun onUpdate(database: ODatabaseDocument?, before: OResult?, after: OResult?) {
            if (before?.equals(after) != true) handleDataRecord(after)
        }

        override fun onDelete(database: ODatabaseDocument?, data: OResult?) {
            log("onDelete: should not happen, or should it?")
        }

        override fun onEnd(database: ODatabaseDocument?) {
            log("ending live broadcasting")
        }


    }


}



