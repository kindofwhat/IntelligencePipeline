package orientdb

import datatypes.Chunk
import datatypes.DataRecord
import datatypes.DocumentRepresentation
import datatypes.Metadata
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

class ChunkCreated(record: DataRecord, val chunk: Chunk, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)
class ChunkUpdated(record: DataRecord, val chunk: Chunk, handledBy: Set<String>) : Command<DataRecord>(record, handledBy)


@ExperimentalCoroutinesApi
@UseExperimental(ImplicitReflectionSerializer::class)
abstract class ChannelIntelligencePipeline: IIntelligencePipeline, CoroutineScope {
    lateinit var job: Job
    override val coroutineContext: CoroutineContext
        //get() = Dispatchers.Default + job
        get() = Executors.newFixedThreadPool(500).asCoroutineDispatcher() + job


    override val registry = DefaultCapabilityRegistry()

    val ingestors = mutableListOf<PipelineIngestor>()
    val metadataProducers = mutableMapOf<UUID, MetadataProducer>()
    val chunkProducers = mutableMapOf<UUID, ChunkProducer>()
    val documentRepresentationProducers = mutableMapOf<UUID, DocumentRepresentationProducer>()
    val ingestionChannel = Channel<DocumentRepresentation>(Int.MAX_VALUE)

    val commandChannel = BroadcastChannel<Command<DataRecord>>(10_000)




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


    override fun registerChunkMetadataProducer(producer: ChunkMetadataProducer) {
        TODO("not implemented")
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
        commandChannel.close()

    }

    override fun run() {
        job = Job()
        launchPerister()
        launchParticipants()
        launchPersister()
        launchDigestConsumers()
    }

    private fun launchPerister() {
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
                commandChannel.send(DataRecordUpdated(dataRecord, emptySet()))
            }
        }
    }

    private fun launchParticipants() {
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
                                val metadata = producer.produce(record)
                                if(metadata != null)
                                    commandChannel.send(MetadataUpdated(command.record, metadata, handledBy =  command.handledBy + uuid.toString()))

                            }
                        }
                        documentRepresentationProducers.filter { (uuid, _) ->
                            !command.handledBy.contains(uuid.toString())
                        }.map { (uuid, producer) ->
                            launch {
                                val docRep = producer.produce(record)
                                if(docRep != null && !command.record.additionalRepresentations.contains(docRep))
                                    commandChannel.send(AdditionalDocumentRepresentationsUpdated(command.record, docRep, handledBy = command.handledBy + uuid.toString()))

                            }
                        }
                        chunkProducers.filter { (uuid, _) ->
                            //TODO: find out how to prevent double calculations
                            true
                        }.map { (uuid, producer) ->
                            launch {
                                producer.produce(command.record)?.forEach { chunk ->
                                    commandChannel.send(ChunkCreated(command.record, chunk, handledBy = command.handledBy
                                            + "${chunk.parent.name}-${chunk.index}-$uuid"))
                                }

                            }
                        }
                    }
                }
            }
        }
    }

    abstract fun launchPersister()


}


