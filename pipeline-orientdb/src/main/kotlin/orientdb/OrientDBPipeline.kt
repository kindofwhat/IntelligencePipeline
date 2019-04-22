package orientdb

import com.orientechnologies.common.exception.OException
import com.orientechnologies.orient.core.db.*
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.executor.OResult
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
        loaders.put(MetadataContainer::class.simpleName ?: "", NullLoader())
        loaders.put(Chunk::class.simpleName ?: "") { session, c ->
            val chunk = c as Chunk
            var res: ODocument? = null
            session.activateOnCurrentThread()
            session.reload()
            val result = session.query("SELECT FROM Chunk WHERE index = ${chunk.index}  " +
                    "and parent.name = '${chunk.parent.name}' and type='${chunk.type}'")
            if (result.hasNext()) {
                val ores = result.next()
                val existingDocument = ores.toElement() as ODocument
                result.close()
                res = existingDocument
            }
            res
        }
        loaders.put(Metadata::class.simpleName ?: "") { session, m ->
            val metadata = m as Metadata
            var res: ODocument? = null
            var query: String? = null
            val container = metadata.container

            when (container) {
                is DataRecord -> query = "SELECT FROM Metadata WHERE createdBy = '${metadata.createdBy}'  " +
                        "and container.name = '${container.name}'"
                is Chunk -> query = "SELECT FROM Metadata WHERE createdBy = ${metadata.createdBy}  " +
                        "and container.parent.name = '${container.parent.name}' " +
                        "and container.type = '${container.type}' " +
                        "and container.index = ${container.index}"
            }
            if (query != null) {
                session.activateOnCurrentThread()
                session.reload()
                val result = session.query(query)
                if (result.hasNext()) {
                    val ores = result.next()
                    val existingDocument = ores.toElement() as ODocument
                    result.close()
                    res = existingDocument
                }
            }

            res
        }


        korient = KOrient(connection, dbName, user, password, loaders)
        orient.createIfNotExists(dbName, ODatabaseType.MEMORY)
        korient.createSchema(DataRecord::class)
        korient.createSchema(Chunk::class)
        korient.createSchema(Metadata::class)

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


    /**
     * watch the pipeline "live"
     */
    private fun liveSubscriber(session: ODatabaseSession,
                               consumer: RecordConsumer) {
        session.live("LIVE SELECT FROM ${DataRecord::class.simpleName}", DataRecordsLiveQueryListener(korient, consumer))
    }


    override fun dataRecords(id: String): ReceiveChannel<DataRecord> {
        val channel = Channel<DataRecord>()

        val session = orient.open(dbName, user, password)
        session.activateOnCurrentThread()
        val res = session.query("SELECT FROM ${DataRecord::class.simpleName}")

        while (res.hasNext()) {
            val maybeDataRecord = korient.toObject(res.next(), DataRecord::class)
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
            val dataRecord = korient.toObject(data, DataRecord::class)

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



