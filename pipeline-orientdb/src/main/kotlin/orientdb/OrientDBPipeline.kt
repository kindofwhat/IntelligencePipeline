package orientdb

import com.orientechnologies.common.exception.OException
import com.orientechnologies.orient.core.db.*
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.impl.ODocument
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

typealias ActionDecider = (DataRecord) -> Boolean
typealias RecordManipulator = (DataRecord) -> DataRecord

@UseExperimental(ImplicitReflectionSerializer::class)
class OrientDBPipeline(connection: String, val dbName: String, val user: String, val password: String) : IIntelligencePipeline, CoroutineScope {
    lateinit var job: Job
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + job


    val orient: OrientDB
    val korient:KOrient
    override val registry = DefaultCapabilityRegistry()

    val ingestors = mutableListOf<PipelineIngestor>()
    val ingestionChannel = Channel<DocumentRepresentation>(Int.MAX_VALUE)


    private val pool: ODatabasePool

    init {

        orient = OrientDB(connection, user, password, OrientDBConfig.defaultConfig())
        pool = ODatabasePool(orient,dbName,user,password)
        val loaders = mutableMapOf<String, Loader<Any>>()
        loaders.put(DataRecord::class.simpleName?:"", FieldLoader("name"))
        loaders.put(DocumentRepresentation::class.simpleName?:"", FieldLoader("path"))
        //loaders.put(Metadata::class.simpleName?:"", FieldLoader("path"))
        loaders.put(Chunk::class.simpleName?:"")  { session,  c ->
            val chunk = c as Chunk
            var res:ODocument? = null
            val result = session.query("SELECT FROM Chunk WHERE index = ${chunk.index}  and parentId = ${chunk.parentId}")
            if (result.hasNext()) {
                val ores = result.next()
                val existingDocument = ores.toElement() as ODocument
                result.close()
                res = existingDocument
            }
            res
        }


        korient = KOrient(connection, dbName, user, password,loaders)
        if(!orient.exists(dbName)) {
            orient.createIfNotExists(dbName, ODatabaseType.MEMORY)
            korient.createSchema(DataRecord::class)
            korient.createSchema(Chunk::class)
        }

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
        session.live("LIVE SELECT FROM ${DataRecord::class.simpleName}", DataRecordsLiveQueryListener(session, korient, actionDecider, recordManipulator))
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
            korient.save(res)
            res
        })
    }

    override fun registerChunkMetadataProducer(producer: ChunkMetadataProducer) {
        TODO("not implemented") //To change body of created korient use File | Settings | File Templates.
    }

    override fun registerChunkProducer(name: String, chunkProducer: ChunkProducer) {
        val session = orient.open(dbName, user, password)
        liveSubscriber(session, { dataRecord ->
            true
        }, { dataRecord ->
            launch {
                chunkProducer.chunks(dataRecord, idForDataRecord(dataRecord)).forEachIndexed { index, chunk ->

                    korient.save( chunk)
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
        TODO("not implemented") //To change body of created korient use File | Settings | File Templates.
    }

    override fun registerDocumentRepresentationProducer(prod: DocumentRepresentationProducer) {
        liveSubscriber(pool.acquire(), { dataRecord ->
            !dataRecord.additionalRepresentations.any { representation -> representation.createdBy == prod.name }
        }, { dataRecord ->
            val representation = prod.documentRepresentationFor(dataRecord)
            val res = dataRecord.copy(additionalRepresentations = dataRecord.additionalRepresentations + representation)
            korient.save(res)
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
                val dataRecord = DataRecord(name = doc.path, representation = doc)
                korient.save(dataRecord)
            }
        }
    }


    override fun dataRecords(id: String): ReceiveChannel<DataRecord> {
        val channel = Channel<DataRecord>()

        val session = orient.open(dbName, user, password)
        session.activateOnCurrentThread()
        val res = session.query("SELECT FROM ${DataRecord::class.simpleName}")

        while (res.hasNext()) {
            val maybeDataRecord = korient.toObject(res.next(), DataRecord::class.java)
            if (maybeDataRecord != null) {
                launch {
                    channel.send(maybeDataRecord)
                }
            }
        }
        return channel
    }

    private class DataRecordsLiveQueryListener(val session: ODatabaseSession,
                                               val korient:KOrient,
                                               val actionDecider: ActionDecider,
                                               val recordManipulator: RecordManipulator) : OLiveQueryResultListener {
        override fun onError(database: ODatabaseDocument?, exception: OException?) {
            log("Exception during live query ${exception}")
        }


        override fun onCreate(database: ODatabaseDocument?, data: OResult?) {
            handleDataRecord(data)
        }

        private fun handleDataRecord(data: OResult?) {
            val dataRecord = korient.toObject(data, DataRecord::class.java)

            if (dataRecord != null && actionDecider.invoke(dataRecord)) {
                val newRecord = recordManipulator.invoke(dataRecord)
                if (newRecord != dataRecord) {
                    log("newRecord $newRecord")
                    korient.save(newRecord)
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


