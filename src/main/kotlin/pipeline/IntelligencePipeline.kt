package pipeline

import datatypes.DataRecord
import datatypes.DocumentRepresentation
import datatypes.Metadata
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.runBlocking
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import participants.DocumentRepresentationProducer
import participants.MetadataProducer
import participants.PipelineIngestor
import pipeline.capabilities.Capability
import pipeline.capabilities.DefaultCapabilityRegistry
import pipeline.serialize.KotlinSerde
import pipeline.serialize.serialize
import util.log
import java.util.*


class IntelligencePipeline(val kafkaBootstrap: String, val stateDir:String) {
    companion object {
        val DOCUMENTREPRESENTATION_INGESTION_TOPIC = "document-representation-ingestion"
        val DOCUMENTREPRESENTATION_TOPIC = "document-representation"
        val METADATA_TOPIC = "metadata"
        val DATARECORD_TOPIC = "datarecord"
    }

    val registry=DefaultCapabilityRegistry()
    val ingestionProducer: Producer<Long, ByteArray>
    val streamsConfig = Properties()
    var streams:KafkaStreams? = null

    val metadataProducerStreams = mutableListOf<KafkaStreams>()
    val documentRepresentationProducer = mutableListOf<DocumentRepresentationProducer>()
    val ingestors = mutableListOf<PipelineIngestor>()
    val ingestionChannel = Channel<DocumentRepresentation>(Int.MAX_VALUE)



    init {
        val producerConfig = Properties()
        producerConfig.put("bootstrap.servers", kafkaBootstrap)
        producerConfig.put("client.id", "IntelligencePipelineFileIngestionProducer")
        producerConfig.put("acks", "all")
        producerConfig.put("key.serializer", LongSerializer::class.java)
        producerConfig.put("retries", 0)
        producerConfig.put("value.serializer", ByteArraySerializer::class.java)
        ingestionProducer = KafkaProducer<Long, ByteArray>(producerConfig)

        streamsConfig.put("bootstrap.servers", kafkaBootstrap)
        streamsConfig.put("auto.offset.reset", "earliest")
        // TODO("correct config for state.dir")
        streamsConfig.put("state.dir", stateDir)
        streamsConfig.put("default.key.serde", Serdes.Long().javaClass)
        streamsConfig.put("default.value.serde", Serdes.ByteArray().javaClass)
        streamsConfig.put("cache.max.bytes.buffering", 0)
        streamsConfig.put("internal.leave.group.on.close", true)
        streamsConfig.put("commit.interval.ms", 100)
        //streamsConfig.put("processing.guarantee", "exactly_once")
        streamsConfig.put("application.id", "IntelligencePipeline")
    }

    /**
     * creates an own stream for this producer and starts it
     */
    fun registerMetadataProducer(prod: MetadataProducer) {
        val builder = StreamsBuilder()

        //the generic datarecord stream
        val datarecordStream =
                builder.stream<Long, DataRecord>(DATARECORD_TOPIC,
                Consumed.with(Serdes.LongSerde(),
                        KotlinSerde(DataRecord::class.java)))

        //all MetadataProducers listen on the datarecord topic and produce metadata to the metadata topic
        datarecordStream
                //TODO: add some logic here to retry after a while
                .filter { key, value -> !value.meta.any { metadata ->
                    metadata.createdBy == prod.name } }
                .mapValues { value ->
                    prod.metadataFor(value)
                }.filter { key, value ->
                    //TODO: filter out the those that are exactly the same as before!
                    value.values.isNotEmpty()
                }.to(METADATA_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(Metadata::class.java)))


        val topology = builder.build()

        val myProp = Properties()
        myProp.putAll(streamsConfig)
        myProp.put("application.id", "IntelligencePipeline_" + prod.name)
        val streams = KafkaStreams(topology, myProp )
        streams.start()

        metadataProducerStreams.add(streams)

        if(prod is Capability<*>) {
            registry.register(prod as Capability<*>)
        }
    }

    fun registerDocumentRepresentationProducer(prod: DocumentRepresentationProducer) {
        documentRepresentationProducer.add(prod)
    }

    fun registerIngestor(ingestor: PipelineIngestor) {
        ingestors.add(ingestor)
        async {
            log("start ingestor ")
            ingestor.ingest(ingestionChannel)
            log("done ingestor")
        }
    }


    fun deregisterIngestor(ingestor: PipelineIngestor) {
        ingestors.remove(ingestor)
    }

    fun stop() {
        metadataProducerStreams.forEach { it.close()}
        streams?.close()
    }

    fun run() {
        runBlocking {
            streams = createMainStream()
            streams?.start()
            ingestionChannel.consumeEach { doc ->
                val out = serialize(doc)
                val msg = ingestionProducer.send(ProducerRecord<Long, ByteArray>(DOCUMENTREPRESENTATION_INGESTION_TOPIC, doc.path.hashCode().toLong(), out))
            }
            ingestionChannel.close()

            log("stopping")
        }
    }

    private fun createMainStream(): KafkaStreams {
        val builder = StreamsBuilder()

        //have the documentrepresenation ingestion stream available
        val ingestionStream = builder.stream<Long, DocumentRepresentation>(DOCUMENTREPRESENTATION_INGESTION_TOPIC,
                Consumed.with(Serdes.LongSerde(),
                        KotlinSerde(DocumentRepresentation::class.java)))
        //collect & aggregate everything from  documentpresentation/ingestion  to a DataRecord
        val ingestionDataRecordTable = ingestionStream
                .groupByKey()
                .aggregate(
                        { DataRecord() },
                        { key, newRepresentation, dataRecord ->
                            dataRecord.copy(representation = newRepresentation)
                        },
                        Materialized.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)))

        //signal from ingestion to metadata. This allows the MetadataProducers to work on metadata topic
        ingestionStream.mapValues { value ->
            Metadata(mapOf("stored" to Date().toString(), "createdBy" to value.createdBy, "path" to value.path),
                    "ingestor")
        }.to(METADATA_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(Metadata::class.java)))


        //have the documentrepresenation  stream available
        val documentRepresentationStream = builder.stream<Long, DocumentRepresentation>(DOCUMENTREPRESENTATION_TOPIC,
                Consumed.with(Serdes.LongSerde(),
                        KotlinSerde(DocumentRepresentation::class.java)))

        //collect & aggregate everything from  documentpresentation/ingestion  to a DataRecord
        val documentRepresentationDataRecordTable = documentRepresentationStream
                .groupByKey()
                .aggregate(
                        { DataRecord() },
                        { key, newRepresentation, dataRecord ->
                            dataRecord.copy(additionalRepresentations = dataRecord.additionalRepresentations + newRepresentation)
                        },
                        Materialized.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)))



        //accumulate the DataRecord from ingestion together with those from metadata and those from documentrepresentation
        val dataRecordAccumulator = builder.stream<Long, Metadata>(METADATA_TOPIC,
                Consumed.with(Serdes.LongSerde(),
                        KotlinSerde(Metadata::class.java)))
                .groupByKey()
                .aggregate(
                        { DataRecord() },
                        { key, metadata, dataRecord ->
                            dataRecord.copy(meta = dataRecord.meta + metadata)
                        },
                        Materialized.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)))

        dataRecordAccumulator.join(ingestionDataRecordTable,
                        { meta, document ->
                            meta.copy(representation = document.representation)
                        })
                .toStream().to(DATARECORD_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)))

        dataRecordAccumulator.join(documentRepresentationDataRecordTable,
                        { meta, document ->
                            meta.copy(additionalRepresentations = document.additionalRepresentations)
                        })
                .toStream().to(DATARECORD_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)))

        //the generic datarecord stream
        val datarecordStream = builder.stream<Long, DataRecord>(DATARECORD_TOPIC,
                Consumed.with(Serdes.LongSerde(),
                        KotlinSerde(DataRecord::class.java)))
        /*

         val datarecordTable = datarecordStream.groupByKey().windowedBy(SessionWindows.with(10000))
         datarecordTable.reduce( { old, new -> new })
 */
        //all DocRepProducers listen on the datarecord topic and produce new documentrepresentations to the DOCUMENTREPRESENTATION_TOPIC
        documentRepresentationProducer.forEach { producer ->
            datarecordStream
                    //TODO: add some logic here to retry after a while
                    .filter { key, value -> !value.additionalRepresentations.any { representation -> representation.createdBy == producer.name } }
                    //TODO: filter out the those that are exactly the same as before!
                    .mapValues { datarecord ->
                        producer.documentRepresentationFor(datarecord)
            }.to(DOCUMENTREPRESENTATION_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(DocumentRepresentation::class.java)))

        }

        val topology = builder.build()

        println(topology.describe().toString())
        val streams = KafkaStreams(topology, streamsConfig)
        return streams
    }
}


