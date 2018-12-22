package pipeline.impl

import datatypes.DataRecord
import datatypes.DocumentRepresentation
import facts.Proposer
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.launch
import participants.*
import pipeline.capabilities.DefaultCapabilityRegistry
import util.log
import java.util.concurrent.ConcurrentHashMap


class MapIntelligencePipeline() : pipeline.IIntelligencePipeline {
    override fun <I, U> registerProposer(prod: Proposer<I, U>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override val registry = DefaultCapabilityRegistry()
    val ingestors = mutableListOf<PipelineIngestor>()

    val all = ConcurrentHashMap<Long, DataRecord>()

    val ingestionChannel = Channel<DocumentRepresentation>(Int.MAX_VALUE)
    val dataRecordMessageChannel =  BroadcastChannel<DataRecordMessage>(1024)
    val workers = mutableSetOf<Any>()

    sealed class DataRecordMessage(val id: Long, val dataRecord: DataRecord) {
        class Create(id: Long, dataRecord: DataRecord) : DataRecordMessage(id, dataRecord)
        class Update(id: Long, dataRecord: DataRecord) : DataRecordMessage(id, dataRecord)

        override fun toString(): String {
            return "${javaClass.name}(id=$id, dataRecord=$dataRecord)"
        }

    }

    override fun dataRecords(id:String): ReceiveChannel<datatypes.DataRecord> {
        val channel= Channel<DataRecord>()
        all.values.forEach{GlobalScope.async {channel.send(it)}}
        return channel
    }

    /**
     * creates an own stream for this producer and starts it
     */
    override fun registerChunkMetadataProducer(producer: ChunkMetadataProducer) {

    }

    /**
     * creates an own stream for this producer and starts it
     */
    override fun registerChunkProducer(name: String, chunkProducer: ChunkProducer) {
    }

    /**
     * creates an own stream for this producer and starts it
     */
    override fun registerSideEffect(name: String, sideEffect: PipelineSideEffect) {
        GlobalScope.async {
            dataRecordMessageChannel.consumeEach { dataRecordMessage ->
                sideEffect.invoke(dataRecordMessage.id, dataRecordMessage.dataRecord)
            }
        }
    }


    /**
     * creates an own stream for this producer and starts it
     */
     override fun registerMetadataProducer (prod: MetadataProducer) {
        handleDataRecordMessage( { dataRecord ->
                !dataRecord.meta.any { metaData -> metaData.createdBy == prod.name }
            }, { dataRecord ->
                val metadata = prod.metadataFor(dataRecord)
                dataRecord.copy(meta = dataRecord.meta + metadata)
        })
    }

    /**
     * generic functional handler
     */
    private fun handleDataRecordMessage(actionDecider: (DataRecord)->Boolean,
                                         newDataRecordCreator: (DataRecord) -> DataRecord) {
        GlobalScope.async {
            dataRecordMessageChannel.consumeEach { dataRecordMessage ->
                val dataRecord = dataRecordMessage.dataRecord
                if (actionDecider.invoke(dataRecord)) {
                    val newRecord = newDataRecordCreator.invoke(dataRecord)
                    if (newRecord != dataRecord) {
                        log("newRecord $newRecord")
                        dataRecordMessageChannel.send(DataRecordMessage.Update(dataRecordMessage.id, newRecord))
                    }
                }
            }
        }
    }

    override fun registerDocumentRepresentationProducer(prod: participants.DocumentRepresentationProducer) {
        handleDataRecordMessage( {
            dataRecord ->
            !dataRecord.additionalRepresentations.any { docRep -> docRep.createdBy == prod.name }
        }, {
            dataRecord ->
            val additionalRep = prod.documentRepresentationFor(dataRecord)
            dataRecord.copy(additionalRepresentations = dataRecord.additionalRepresentations + additionalRep)
        })
    }

    override fun registerIngestor(ingestor: participants.PipelineIngestor) {
        ingestors.add(ingestor)
        GlobalScope.async {
            log("start ingestor ")
            ingestor.ingest(ingestionChannel)
            log("done ingestor")
        }
    }

    override fun stop() {

        ingestionChannel.close()
        dataRecordMessageChannel.close()
    }

    override fun run() {

        GlobalScope.launch {
            ingestionChannel.consumeEach { doc ->
                val dataRecord = DataRecord(name = doc.path, representation = doc)
                dataRecordMessageChannel.send(DataRecordMessage.Create(doc.path.hashCode().toLong(), dataRecord))
            }
        }
        ingestionChannel.close()

        registerSideEffect("putting") { k, v ->
            log("putting $v")
            //TODO: somehow make sure always the last result is stored
            all.set(k, v)
        }
    }
}


