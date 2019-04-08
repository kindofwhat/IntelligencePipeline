package orientdb

import com.orientechnologies.common.exception.OException
import com.orientechnologies.orient.core.db.*
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.executor.OResult
import datatypes.Chunk
import datatypes.DataRecord
import datatypes.DocumentRepresentation
import datatypes.Metadata
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import kotlinx.serialization.ImplicitReflectionSerializer
import pipeline.capabilities.DefaultCapabilityRegistry
import util.log

typealias RecordConsumer = (DataRecord) -> Unit


abstract class Command<T>(var record: T, var handledBy: Set<String>)
class DataRecordUpdated(record: DataRecord, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)

class AdditionalDocumentRepresentationsUpdated(record: DataRecord, val documentRepresentations: DocumentRepresentation, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)
class MetadataUpdated(record: DataRecord, val meta: Metadata, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)

class ChunkCreated(record: DataRecord, val chunk: Chunk, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)
class ChunkUpdated(record: DataRecord, val chunk: Chunk, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)


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
        loaders.put(DocumentRepresentation::class.simpleName ?: "", FieldLoader("path"))
        //loaders.put(Metadata::class.simpleName?:"", FieldLoader("path"))
        loaders.put(Chunk::class.simpleName ?: "") { session, c ->
            val chunk = c as Chunk
            var res: ODocument? = null
            val result = session.query("SELECT FROM Chunk WHERE index = ${chunk.index}  and parentId = ${chunk.parentId} and type='${chunk.type}'")
            if (result.hasNext()) {
                val ores = result.next()
                val existingDocument = ores.toElement() as ODocument
                result.close()
                res = existingDocument
            }
            res
        }


        korient = KOrient(connection, dbName, user, password, loaders)
        if (!orient.exists(dbName)) {
            orient.createIfNotExists(dbName, ODatabaseType.MEMORY)
            korient.createSchema(DataRecord::class)
            korient.createSchema(Chunk::class)
        }

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

    /*
        override fun dataRecords(id: String): ReceiveChannel<DataRecord> {
        val channel = Channel<DataRecord>()

        val session = orient.open(dbName, user, password)
        liveSubscriber(session, {record -> launch{channel.send(record)}})
        return channel
    }
    */
    override fun launchPersister() {
        launch {
            for (command in commandChannel.openSubscription()) {
                var dbRecord = korient.load(command.record)
                if (dbRecord == null) {
                    dbRecord = korient.saveObject(command.record)
                }

                if (dbRecord != null) {
                    var updatedRecord = dbRecord.copy()
                    when (command) {
                        is MetadataUpdated -> {
                            updatedRecord = updatedRecord.copy(meta = updatedRecord.meta + command.meta)
                        }
                        is AdditionalDocumentRepresentationsUpdated -> {
                            updatedRecord = updatedRecord.copy(additionalRepresentations = updatedRecord.additionalRepresentations + command.documentRepresentations)
                        }
                        is ChunkCreated -> {
                            val savedChunk = korient.saveObject(command.chunk)
                            if (command.chunk != savedChunk && savedChunk != null) {
                                commandChannel.send(ChunkUpdated(command.record, savedChunk, emptySet()))
                            }
                        }
                    }
                    if (!updatedRecord.equals(dbRecord)) {
                        korient.saveObject(updatedRecord)
                        commandChannel.send(DataRecordUpdated(updatedRecord, emptySet()))
                    }
                }
            }
        }

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


