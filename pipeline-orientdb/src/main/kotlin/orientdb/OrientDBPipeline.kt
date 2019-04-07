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
import facts.Proposer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.serialization.ImplicitReflectionSerializer
import participants.*
import pipeline.IIntelligencePipeline
import pipeline.capabilities.DefaultCapabilityRegistry
import util.log
import java.util.*
import java.util.concurrent.Executors
import kotlin.coroutines.CoroutineContext

typealias RecordConsumer = (DataRecord) -> Unit


abstract class Command<T>(var record: T, var handledBy: Set<String>)
class DataRecordUpdated(record: DataRecord, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)
class ChunkCreated(record: DataRecord, chunk: Chunk, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)
class AdditionalDocumentRepresentationsUpdated(record: DataRecord, val documentRepresentations: DocumentRepresentation, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)
class MetadataUpdated(record: DataRecord, val meta: Metadata, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)

@ExperimentalCoroutinesApi
@UseExperimental(ImplicitReflectionSerializer::class)
class OrientDBPipeline(connection: String, val dbName: String, val user: String, val password: String) : IIntelligencePipeline, CoroutineScope {
    lateinit var job: Job
    override val coroutineContext: CoroutineContext
        //get() = Dispatchers.Default + job
        get() = Executors.newFixedThreadPool(500).asCoroutineDispatcher() + job


    val orient: OrientDB
    val korient: KOrient
    override val registry = DefaultCapabilityRegistry()

    val ingestors = mutableListOf<PipelineIngestor>()
    val metadataProducers = mutableMapOf<UUID, MetadataProducer>()
    val chunkProducers = mutableMapOf<UUID, ChunkProducer>()
    val documentRepresentationProducers = mutableMapOf<UUID, DocumentRepresentationProducer>()
    val ingestionChannel = Channel<DocumentRepresentation>(Int.MAX_VALUE)

    val commandChannel = BroadcastChannel<Command<DataRecord>>(10_000)


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
            val result = session.query("SELECT FROM Chunk WHERE index = ${chunk.index}  and parentId = ${chunk.parentId}")
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

    private fun idForDataRecord(dataRecord: DataRecord): Long {
        return dataRecord.representation.path.hashCode().toLong()
    }

    private fun idForChunk(dataRecord: DataRecord, chunkProducer: ChunkProducer, chunk: Chunk): String {
        return "${chunkProducer.name}_${idForDataRecord(dataRecord)}_${chunk.index}"
    }

    /**
     * generic functional handler
     */
    private fun liveSubscriber(session: ODatabaseSession,
                               consumer: RecordConsumer) {
        session.live("LIVE SELECT FROM ${DataRecord::class.simpleName}", DataRecordsLiveQueryListener(korient, consumer))
    }

    /**
     * creates an own stream for this producer and starts it
     */
    override fun registerMetadataProducer(prod: MetadataProducer) {
        metadataProducers.put(UUID.randomUUID(), prod)
    }


    override fun registerChunkMetadataProducer(producer: ChunkMetadataProducer) {
        TODO("not implemented") //To change body of created korient use File | Settings | File Templates.
    }

    override fun registerChunkProducer(name: String, chunkProducer: ChunkProducer) {
        chunkProducers.put(UUID.randomUUID(), chunkProducer)
    }

    override fun registerSideEffect(name: String, sideEffect: PipelineSideEffect) {

        launch {
            for (command in commandChannel.openSubscription()) {
                sideEffect.invoke(-1, command.record)
            }
        }
    }

    override fun <I, U> registerProposer(proposer: Proposer<I, U>) {
        TODO("not implemented") //To change body of created korient use File | Settings | File Templates.
    }

    override fun registerDocumentRepresentationProducer(prod: DocumentRepresentationProducer) {
        documentRepresentationProducers.put(UUID.randomUUID(), prod)
    }

    override fun registerIngestor(ingestor: participants.PipelineIngestor) {
        ingestors.add(ingestor)
    }

    override fun stop() {
        job.cancel()
        ingestionChannel.close()
        commandChannel.close()

    }

    override fun run() {
        job = Job()
        launch {
            ingestors.forEach { ingestor ->
                log("start ingestor ")
                ingestor.ingest(ingestionChannel)
                log("done ingestor")
            }
        }
        launch {
            for (command in commandChannel.openSubscription()) {
                val record = command.record
                log(command.toString())
                when (command) {
                    //this record has been saved in the db
                    is DataRecordUpdated -> {
                        metadataProducers.filter { (uuid, _) ->
                            !command.handledBy.contains(uuid.toString())
                        }.map { (uuid, producer) ->
                            launch {
                                val metadata = producer.metadataFor(record)
                                commandChannel.send(MetadataUpdated(command.record, metadata, handledBy = command.handledBy + uuid.toString()))

                            }
                        }
                        documentRepresentationProducers.filter { (uuid, _) ->
                            !command.handledBy.contains(uuid.toString())
                        }.map { (uuid, producer) ->
                            launch {
                                val docRep = producer.documentRepresentationFor(record)
                                commandChannel.send(AdditionalDocumentRepresentationsUpdated(command.record, docRep, handledBy = command.handledBy + uuid.toString()))

                            }
                        }
                        chunkProducers.filter { (uuid, _) ->
                            !command.handledBy.contains(uuid.toString())
                        }.map { (uuid, producer) ->
                            launch {
                                producer.chunks(command.record, idForDataRecord(command.record)).forEach { chunk ->
                                    commandChannel.send(ChunkCreated(command.record, chunk, handledBy = command.handledBy + "${chunk.parentId}-${chunk.index}-$uuid"))
                                }

                            }
                        }
                    }
                }
            }
        }
        launch {
            for (command in commandChannel.openSubscription()) {
                var dbRecord = korient.load(command.record)
                if (dbRecord == null) {
                    dbRecord = korient.saveObject(command.record)
                }

                if(dbRecord != null) {
                    var updatedRecord = dbRecord.copy()
                    when (command) {
                        is MetadataUpdated -> {
                            updatedRecord = updatedRecord.copy(meta = updatedRecord.meta + command.meta)
                        }
                        is AdditionalDocumentRepresentationsUpdated -> {
                            updatedRecord = updatedRecord.copy(additionalRepresentations = updatedRecord.additionalRepresentations + command.documentRepresentations)
                        }
                        is ChunkCreated -> {

                        }
                    }
                    if (!updatedRecord.equals(dbRecord)) {
                        korient.saveObject(updatedRecord)
                        commandChannel.send(DataRecordUpdated(updatedRecord, emptySet()))
                    }
                }
            }
        }

        launch {

            ingestionChannel.consumeEach { doc ->
                val dataRecord = DataRecord(name = doc.path, representation = doc)
                commandChannel.send(DataRecordUpdated(dataRecord, emptySet()))
            }
        }
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

            if (dataRecord!=null) {
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


