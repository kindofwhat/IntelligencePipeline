package orientdb

import datatypes.*
import facts.Proposer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
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

class AdditionalDocumentRepresentationsUpdated(record: DataRecord, val documentRepresentations: DocumentRepresentation, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)
class MetadataUpdated(record: DataRecord, val meta: Metadata, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)

class ChunkUpdated(record: DataRecord, val chunk: Chunk, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)
class ChunkMetadataUpdated(chunk: Chunk, val meta: Metadata, handledBy: Set<String>) : Command<Chunk>(chunk, handledBy)
class ChunkNamedEntityExtracted(val chunk: Chunk, val ne: NamedEntity, handledBy: Set<String>) : Command<Chunk>(chunk, handledBy)



abstract class ChannelIntelligencePipeline: IIntelligencePipeline, CoroutineScope {

    lateinit var job: Job
    override val coroutineContext: CoroutineContext
        //get() = Dispatchers.Default + job
        get() = Executors.newFixedThreadPool(500).asCoroutineDispatcher() + job


    override val registry = DefaultCapabilityRegistry()

    val ingestors = mutableListOf<PipelineIngestor>()
    val metadataProducers = mutableMapOf<UUID, MetadataProducer>()
    val chunkProducers = mutableMapOf<UUID, ChunkProducer>()
    val chunkMetadataProducers = mutableMapOf<UUID, ChunkMetadataProducer>()
    val documentRepresentationProducers = mutableMapOf<UUID, DocumentRepresentationProducer>()
    val chunkNEExtractors = mutableMapOf<UUID, ChunkNamedEntityExtractor>()



    val ingestionChannel = Channel<DocumentRepresentation>(Int.MAX_VALUE)

    val dataRecordCommandChannel = BroadcastChannel<Command<DataRecord>>(10_000)
    val chunkCommandChannel = BroadcastChannel<Command<Chunk>>(10_000)




    private fun idForDataRecord(dataRecord: DataRecord): Long {
        return dataRecord.representation.path.hashCode().toLong()
    }

    private fun idForChunk(dataRecord: DataRecord, chunkProducer: ChunkProducer, chunk: Chunk): String {
        return "${chunkProducer.name}_${idForDataRecord(dataRecord)}_${chunk.index}"
    }


    /**
     * creates an own stream for this producer and starts it
     */
    override fun registerMetadataProducer(prod: MetadataProducer) {
        metadataProducers.put(UUID.randomUUID(), prod)
    }

    override fun registerChunkNamedEntityExtractor(extractor: ChunkNamedEntityExtractor) {
        chunkNEExtractors.put(UUID.randomUUID(), extractor)
    }


    override fun registerChunkMetadataProducer(producer: ChunkMetadataProducer) {
        chunkMetadataProducers.put(UUID.randomUUID(), producer)
    }

    override fun registerChunkProducer(name: String, chunkProducer: ChunkProducer) {
        chunkProducers.put(UUID.randomUUID(), chunkProducer)
    }

    override fun registerSideEffect(name: String, sideEffect: PipelineSideEffect) {

        launch {
            for (command in dataRecordCommandChannel.openSubscription()) {
                sideEffect.invoke(-1, command.record)
            }
        }
    }

    override fun <I, U> registerProposer(proposer: Proposer<I, U>) {
        TODO("not implemented")
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
        dataRecordCommandChannel.close()
        chunkCommandChannel.close()

    }

    override fun run() {
        job = Job()
        launchIngestors()
        launchParticipants()
        launchPersister()
        launchDigestConsumers()
    }

    private fun launchIngestors() {
        launch {
            ingestors.forEach { ingestor ->
                log("start ingestor ")
                ingestor.ingest(ingestionChannel)
                log("done ingestor")
            }
        }
    }

    private fun launchDigestConsumers() {
        launch {

            ingestionChannel.consumeEach { doc ->

                val dataRecord = DataRecord(name = doc.path, representation = doc)
                dataRecordCommandChannel.send(DataRecordUpdated(dataRecord, emptySet()))
            }
        }
    }

    private fun launchParticipants() {
        launch {
            for (command in dataRecordCommandChannel.openSubscription()) {
                val record = command.record
                log(command.toString())
                when (command) {
                    //this record has been saved in the db
                    is DataRecordUpdated -> {
                        metadataProducers.filter { (uuid, _) ->
                            !command.handledBy.contains(uuid.toString())
                        }.map { (uuid, producer) ->
                            launch {
                                val metadata = producer.produce(record)
                                if(metadata != null)
                                    dataRecordCommandChannel.send(MetadataUpdated(command.record, metadata, handledBy =  command.handledBy + uuid.toString()))

                            }
                        }
                        documentRepresentationProducers.filter { (uuid, _) ->
                            !command.handledBy.contains(uuid.toString())
                        }.map { (uuid, producer) ->
                            launch {
                                val docRep = producer.produce(record)
                                if(docRep != null && !command.record.additionalRepresentations.contains(docRep))
                                    dataRecordCommandChannel.send(AdditionalDocumentRepresentationsUpdated(command.record, docRep, handledBy = command.handledBy + uuid.toString()))

                            }
                        }
                        chunkProducers.filter { (uuid, _) ->
                            //TODO: find out how to prevent double calculations
                            true
                        }.map { (uuid, producer) ->
                            launch {
                                producer.produce(command.record)?.forEach { chunk ->
                                    dataRecordCommandChannel.send(ChunkUpdated(command.record, chunk, handledBy = command.handledBy
                                            + "${chunk.parent.name}-${chunk.index}-$uuid"))
                                }

                            }
                        }
                    }
                    is ChunkUpdated -> {
                        chunkMetadataProducers.filter {  (uuid, _) ->
                            !command.handledBy.contains(uuid.toString())
                        }.map  { (uuid, producer) ->
                            launch {
                                val metadata = producer.produce(command.chunk)
                                if(metadata != null)
                                    chunkCommandChannel.send(ChunkMetadataUpdated(command.chunk, metadata, handledBy =  command.handledBy + uuid.toString()))
                            }
                        }
                        chunkNEExtractors.filter {  (uuid, _) ->
                            !command.handledBy.contains(uuid.toString())
                        }.map  { (uuid, extractor) ->
                            launch {
                                extractor.invoke(command.chunk).forEach { ne -> chunkCommandChannel.send(ChunkNamedEntityExtracted(command.chunk, ne, handledBy =  command.handledBy + uuid.toString()))}

                            }
                        }
                    }
                }
            }
        }
    }

    /*
    override fun dataRecords(id: String): ReceiveChannel<DataRecord> {
    val channel = Channel<DataRecord>()

    val session = orient.open(dbName, user, password)
    liveSubscriber(session, {record -> launch{channel.send(record)}})
    return channel
}
*/
    abstract fun loadDataRecord(value:DataRecord): DataRecord?
    abstract fun saveDataRecord(value:DataRecord): DataRecord?

    abstract fun loadMetadata(value:Metadata): Metadata?
    abstract fun saveMetadata(value:Metadata): Metadata?

    abstract fun loadChunk(value:Chunk): Chunk?
    abstract fun saveChunk(value:Chunk): Chunk?

    abstract fun saveNamedEntity(chunk: Chunk, ne: NamedEntity): NamedEntityRelation?



    fun launchPersister() {
        launch {
            for (command in dataRecordCommandChannel.openSubscription()) {
                var dbRecord = loadDataRecord(command.record)
                if (dbRecord == null) {
                    dbRecord = saveDataRecord(command.record)
                }

                if (dbRecord != null) {
                    var updatedRecord: DataRecord = dbRecord.copy()
                    when (command) {
                        is MetadataUpdated -> {
                            val meta = command.meta.copy(container = dbRecord)
                            saveMetadata(meta)
                        }
                        is AdditionalDocumentRepresentationsUpdated -> {
                            updatedRecord = updatedRecord.copy(additionalRepresentations = updatedRecord.additionalRepresentations + command.documentRepresentations)
                        }
                        is ChunkUpdated -> {
                            val chunk = command.chunk.copy(parent = dbRecord)
                           saveChunk(chunk)
                        }
                    }
                    if (!updatedRecord.equals(dbRecord)) {
                        val savedRecord = saveDataRecord(updatedRecord)
                        if (savedRecord != null)
                            dataRecordCommandChannel.send(DataRecordUpdated(updatedRecord, emptySet()))
                        else
                            println("something went wrong while persisting $updatedRecord")
                    }
                }
            }
        }
        launch {
            for (command in chunkCommandChannel.openSubscription()) {
                var myChunk: Chunk? =loadChunk(command.record)
                if (myChunk == null) {
                    myChunk =saveChunk(command.record)
                }
                if (myChunk != null) {
                    var updatedRecord: Chunk = myChunk.copy()
                    when (command) {
                        is ChunkMetadataUpdated -> {
                            saveMetadata(command.meta)
                        }
                        is ChunkNamedEntityExtracted -> {
                            saveNamedEntity(command.chunk, command.ne)
                        }
                    }
                }
            }
        }
    }



}


