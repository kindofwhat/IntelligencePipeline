package orientdb

import com.orientechnologies.common.exception.OException
import com.orientechnologies.orient.core.db.*
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.sql.executor.OResult
import datatypes.Chunk
import datatypes.DataRecord
import datatypes.DocumentRepresentation
import facts.Proposer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.serialization.*
import participants.*
import pipeline.IIntelligencePipeline
import pipeline.capabilities.DefaultCapabilityRegistry
import util.log
import kotlin.coroutines.CoroutineContext

@Serializable
data class OrientDataRecord(val id: String = "", val dataRecord: DataRecord = DataRecord())
@Serializable
data class OrientChunk(val id: String = "", val chunk: Chunk = Chunk())

typealias ActionDecider = (DataRecord) -> Boolean
typealias RecordManipulator = (DataRecord) -> DataRecord

@UseExperimental(ImplicitReflectionSerializer::class)
class OrientDBPipeline(connection: String, val dbName: String, val user: String, val password: String) : IIntelligencePipeline, CoroutineScope {
    lateinit var job: Job
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + job


    val orient: OrientDB
    override val registry = DefaultCapabilityRegistry()

    val ingestors = mutableListOf<PipelineIngestor>()
    val ingestionChannel = Channel<DocumentRepresentation>(Int.MAX_VALUE)


    private val pool: ODatabasePool

    init {

        orient = OrientDB(connection, user, password, OrientDBConfig.defaultConfig())
        pool = ODatabasePool(orient,dbName,user,password)
        orient.createIfNotExists(dbName, ODatabaseType.MEMORY)
        val session = orient.open(dbName, user, password)

        createOrientDocumentClassWithIndex(session, OrientDataRecord::class.simpleName ?: "OrientDataRecord", "id")
        createOrientDocumentClassWithIndex(session, OrientChunk::class.simpleName ?: "OrientChunk", "id")
    }

    private fun idForDataRecord(dataRecord: DataRecord): Long {
        return dataRecord.representation.path.hashCode().toLong();
    }

    private fun idForChunk(dataRecord: DataRecord, chunkProducer: ChunkProducer, chunk: Chunk): String {
        return "${chunkProducer.name}_${idForDataRecord(dataRecord)}_${chunk.index}"
    }
    /**
     * generic functional handler
     */
    private fun liveSubscriber(session: ODatabaseSession,
                               actionDecider: ActionDecider,
                               recordManipulator: RecordManipulator) {
        session.live("LIVE SELECT FROM ${OrientDataRecord::class.simpleName}", DataRecordsLiveQueryListener(session, actionDecider, recordManipulator))
    }

    /**
     * creates an own stream for this producer and starts it
     */
    override fun registerMetadataProducer(prod: MetadataProducer) {
        val session = orient.open(dbName, user, password)
        liveSubscriber(session, { dataRecord ->
            !dataRecord.meta.any { metaData -> metaData.createdBy == prod.name }
        }, { dataRecord ->
            val metadata = prod.metadataFor(dataRecord)
            val res = dataRecord.copy(meta = dataRecord.meta + metadata)
            persist(session, OrientDataRecord("" + idForDataRecord(dataRecord), res), "id")
            res
        })
    }

    override fun registerChunkMetadataProducer(producer: ChunkMetadataProducer) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun registerChunkProducer(name: String, chunkProducer: ChunkProducer) {
        val session = orient.open(dbName, user, password)
        liveSubscriber(session, { dataRecord ->
            //TODO: how to find out if the producer has already run?
            true
        }, { dataRecord ->
            launch {
                chunkProducer.chunks(dataRecord, idForDataRecord(dataRecord)).forEachIndexed { index, chunk ->

                    persist(pool.acquire(), OrientChunk (idForChunk(dataRecord,chunkProducer,chunk),chunk), "id")
                }
            }
            dataRecord
        })
    }

    override fun registerSideEffect(name: String, sideEffect: PipelineSideEffect) {

        liveSubscriber(pool.acquire(), { dataRecord -> true }, { dataRecord ->
            sideEffect.invoke(0L, dataRecord)
            dataRecord
        })
    }

    override fun <I, U> registerProposer(proposer: Proposer<I, U>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun registerDocumentRepresentationProducer(prod: DocumentRepresentationProducer) {
        liveSubscriber(pool.acquire(), { dataRecord ->
            !dataRecord.additionalRepresentations.any { representation -> representation.createdBy == prod.name }
        }, { dataRecord ->
            val representation = prod.documentRepresentationFor(dataRecord)
            val res = dataRecord.copy(additionalRepresentations = dataRecord.additionalRepresentations + representation)
            persist(pool.acquire(), OrientDataRecord("" + idForDataRecord(dataRecord), res), "id")
            res
        })
    }

    override fun registerIngestor(ingestor: participants.PipelineIngestor) {
        ingestors.add(ingestor)
    }

    override fun stop() {
        job.cancel()
        ingestionChannel.close()

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
            ingestionChannel.consumeEach { doc ->
                val dataRecord = OrientDataRecord("" + doc.path.hashCode(), DataRecord(name = doc.path, representation = doc))

                persist(pool.acquire(), dataRecord, "id")
            }
        }
    }


    override fun dataRecords(id: String): ReceiveChannel<DataRecord> {
        val channel = Channel<DataRecord>()

        val session = orient.open(dbName, user, password)
        session.activateOnCurrentThread()
        val res = session.query("SELECT FROM ${OrientDataRecord::class.simpleName}")

        while (res.hasNext()) {
            val maybeDataRecord = toObject(res.next(), OrientDataRecord::class.java)
            if (maybeDataRecord != null) {
                launch {
                    channel.send(maybeDataRecord.dataRecord)
                }
            }
        }
        return channel
    }

    private class DataRecordsLiveQueryListener(val session: ODatabaseSession,
                                               val actionDecider: ActionDecider,
                                               val recordManipulator: RecordManipulator) : OLiveQueryResultListener {
        override fun onError(database: ODatabaseDocument?, exception: OException?) {
            log("Exception during live query ${exception}")
        }


        override fun onCreate(database: ODatabaseDocument?, data: OResult?) {
            handleDataRecord(data)
        }

        private fun handleDataRecord(data: OResult?) {
            val orientRecord = toObject(data, OrientDataRecord::class.java)
            val dataRecord = orientRecord?.dataRecord

            if (dataRecord != null && actionDecider.invoke(dataRecord)) {
                val newRecord = recordManipulator.invoke(dataRecord)
                if (newRecord != dataRecord) {
                    log("newRecord $newRecord")
                    val persistedRecord = OrientDataRecord(orientRecord.id, newRecord)
                    persist(session, persistedRecord, "id")
                }
            }

        }

        override fun onUpdate(database: ODatabaseDocument?, before: OResult?, after: OResult?) {
            if (!(before?.equals(after) ?: false)) handleDataRecord(after)
        }

        override fun onDelete(database: ODatabaseDocument?, data: OResult?) {
            log("onDelete: should not happen, or should it?")
        }

        override fun onEnd(database: ODatabaseDocument?) {
            log("ending live broadcasting")
        }


    }


}


