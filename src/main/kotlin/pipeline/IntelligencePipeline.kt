package pipeline

import datatypes.*
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.runBlocking
import org.apache.commons.lang.StringUtils
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
import participants.*
import pipeline.capabilities.Capability
import pipeline.capabilities.DefaultCapabilityRegistry
import pipeline.serialize.KotlinSerde
import pipeline.serialize.serialize
import util.log
import java.util.*


class IntelligencePipeline(kafkaBootstrap: String, stateDir:String, val applicationId:String ="IntelligencePipeline") {
    companion object {
        val DOCUMENTREPRESENTATION_INGESTION_TOPIC = "document-representation-ingestion"
        val DOCUMENTREPRESENTATION_EVENT_TOPIC = "document-representation-event"
        val METADATA_EVENT_TOPIC = "metadata-event"
        val DATARECORD_EVENT_TOPIC = "datarecord-event"
        val DATARECORD_CONSOLIDATED_TOPIC = "datarecord-consolidated"
        val CHUNK_TOPIC = "chunk"
    }

    val registry=DefaultCapabilityRegistry()
    val ingestionProducer: Producer<Long, ByteArray>
    val streamsConfig = Properties()
    var streams:KafkaStreams? = null

    val subStreams = mutableListOf<KafkaStreams>()

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
        streamsConfig.put("application.id", applicationId)
    }

    /**
     * creates an own stream for this producer and starts it
     */
    fun registerChunkProducer(name:String,chunkProducer: ChunkProducer) {
        val builder = StreamsBuilder()

        //the generic datarecord stream
        val datarecordStream =
                builder.stream<Long, DataRecord>(DATARECORD_CONSOLIDATED_TOPIC,
                        Consumed.with(Serdes.LongSerde(),
                                KotlinSerde(DataRecord::class.java)))

        //act on the default representation
        datarecordStream
                .filter { key, value ->  value != null}
                .flatMapValues { value: DataRecord -> runBlocking { chunkProducer.chunks(value) }.asIterable() }
                .to(CHUNK_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(Chunk::class.java)))

        val topology = builder.build()

        val myProp = Properties()
        myProp.putAll(streamsConfig)
        myProp.put("application.id", applicationId + "_chunk_" + name)
        val streams = KafkaStreams(topology, myProp )
        streams.start()

        subStreams.add(streams)
    }
    /**
     * creates an own stream for this producer and starts it
     */
    fun registerSideEffect(name:String, sideEffect: PipelineSideEffect) {
        val builder = StreamsBuilder()

        //the generic datarecord stream
        val datarecordStream =
                builder.stream<Long, DataRecord>(DATARECORD_CONSOLIDATED_TOPIC,
                        Consumed.with(Serdes.LongSerde(),
                                KotlinSerde(DataRecord::class.java)))

        //all MetadataProducers listen on the datarecord topic and produce metadata to the metadata topic
        datarecordStream.foreach { key, value ->  sideEffect(key, value)}


        val topology = builder.build()

        val myProp = Properties()
        myProp.putAll(streamsConfig)
        myProp.put("application.id", applicationId + "_sideeffect_" + name)
        val streams = KafkaStreams(topology, myProp )
        streams.start()

        subStreams.add(streams)
    }

    /**
     * creates an own stream for this producer and starts it
     */
    fun registerMetadataProducer(prod: MetadataProducer) {
        val builder = StreamsBuilder()

        //the generic datarecord stream
        val datarecordStream =
                builder.stream<Long, DataRecord>(DATARECORD_CONSOLIDATED_TOPIC,
                Consumed.with(Serdes.LongSerde(),
                        KotlinSerde(DataRecord::class.java)))

        //all MetadataProducers listen on the datarecord topic and produce metadata to the metadata topic
        datarecordStream
                //TODO: add some logic here to retry after a while
                .filter { key, value -> !value.meta.any { metadata ->
                    metadata.createdBy == prod.name } }
                .mapValues { value ->
                    MetadataEvent(BaseCommand.UPSERT,prod.metadataFor(value))
/*
                    val job= async {
                        MetadataEvent(BaseCommand.UPSERT,prod.metadataFor(value))

                    }
                    runBlocking { job.await() }
*/
                }.filter { key, value ->
                    //TODO: filter out the those that are exactly the same as before!
                    value.record.values.isNotEmpty()
                }.to(METADATA_EVENT_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(MetadataEvent::class.java)))


        val topology = builder.build()

        val myProp = Properties()
        myProp.putAll(streamsConfig)
        myProp.put("application.id", applicationId + "_metadata_" + prod.name)
        val streams = KafkaStreams(topology, myProp )
        streams.start()

        subStreams.add(streams)

        if(prod is Capability<*>) {
            registry.register(prod as Capability<*>)
        }
    }

    fun registerDocumentRepresentationProducer(prod: DocumentRepresentationProducer) {
        val builder = StreamsBuilder()
        //the generic datarecord stream
        val datarecordStream = builder.stream<Long, DataRecord>(DATARECORD_CONSOLIDATED_TOPIC,
                Consumed.with(Serdes.LongSerde(),
                        KotlinSerde(DataRecord::class.java)))
        //all DocRepProducers listen on the datarecord topic and produce new documentrepresentations to the DOCUMENTREPRESENTATION_TOPIC
        datarecordStream
                //TODO: add some logic here to retry after a while
                .filter { key, value -> !value.additionalRepresentations.any { representation -> representation.createdBy == prod.name } }
                //TODO: filter out the those that are exactly the same as before!
                .mapValues { datarecord ->
                    prod.documentRepresentationFor(datarecord)
                }.filter { key, value ->
                    //TODO: filter out the those that are exactly the same as before!
                    StringUtils.isNotEmpty(value.path)
                }.mapValues { value -> DocumentRepresentationEvent(record = value) }
                .to(DOCUMENTREPRESENTATION_EVENT_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(DocumentRepresentationEvent::class.java)))

        val topology = builder.build()

        val myProp = Properties()
        myProp.putAll(streamsConfig)
        myProp.put("application.id", applicationId + "_document_representation_" + prod.name)
        val streams = KafkaStreams(topology, myProp )
        streams.start()

        subStreams.add(streams)
    }

    fun registerIngestor(ingestor: PipelineIngestor) {
        ingestors.add(ingestor)
        async {
            log("start ingestor ")
            ingestor.ingest(ingestionChannel)
            log("done ingestor")
        }
    }

    fun stop() {
        subStreams.forEach { it.close()}
        streams?.close()
    }

    fun run() {
        runBlocking {
            streams = createMainStream()
            streams?.start()
            ingestionChannel.consumeEach { doc ->
                val out = serialize(doc)
                ingestionProducer.send(ProducerRecord<Long, ByteArray>(DOCUMENTREPRESENTATION_INGESTION_TOPIC, doc.path.hashCode().toLong(), out))
            }
            ingestionChannel.close()

            log(    "stopping")
        }
    }
    private fun createMainStream(): KafkaStreams {
        val builder = StreamsBuilder()
        val ingestionStream = builder.stream<Long, DocumentRepresentation>(DOCUMENTREPRESENTATION_INGESTION_TOPIC,
                Consumed.with(Serdes.LongSerde(),
                        KotlinSerde(DocumentRepresentation::class.java)))

        ingestionStream.mapValues { documentRepresentation ->
            DataRecordEvent(DataRecordCommand.CREATE,DataRecord(representation = documentRepresentation, name = documentRepresentation.path))
        }.to(DATARECORD_EVENT_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(DataRecordEvent::class.java)))


        val documentRepresentationEventStream = builder.stream<Long, DocumentRepresentationEvent>(DOCUMENTREPRESENTATION_EVENT_TOPIC,
                Consumed.with(Serdes.LongSerde(),
                        KotlinSerde(DocumentRepresentationEvent::class.java)))

        documentRepresentationEventStream.mapValues { value ->
            if(value.command==BaseCommand.UPSERT) {
                DataRecordEvent(DataRecordCommand.UPSERT_DOCUMENT_REPRESENTATION, DataRecord(additionalRepresentations = setOf(value.record)))
            } else{
                throw Exception("Don't know how to handle $value")
            }
        }.to(DATARECORD_EVENT_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(DataRecordEvent::class.java)))


        val metadataEventStream = builder.stream<Long, MetadataEvent>(METADATA_EVENT_TOPIC,
                Consumed.with(Serdes.LongSerde(),
                        KotlinSerde(MetadataEvent::class.java)))

        metadataEventStream.mapValues { value ->
            if(value.command==BaseCommand.UPSERT) {
                DataRecordEvent(DataRecordCommand.UPSERT_METADATA, DataRecord(meta = setOf(value.record)))
            } else{
                throw Exception("Don't know how to handle $value")
            }
        }.to(DATARECORD_EVENT_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(DataRecordEvent::class.java)))

        val dataRecordEventStream = builder.stream<Long, DataRecordEvent>(DATARECORD_EVENT_TOPIC,
                Consumed.with(Serdes.LongSerde(),
                        KotlinSerde(DataRecordEvent::class.java)))

        val dataRecordTable = dataRecordEventStream
                .groupByKey()
                .aggregate(
                        { DataRecord() },
                        { key, dataRecordEvent, dataRecord ->
                            if(dataRecordEvent.command == DataRecordCommand.CREATE) {
                                dataRecord.copy(representation = dataRecordEvent.record.representation, name = dataRecordEvent.record.name)
                            } else if(dataRecordEvent.command == DataRecordCommand.UPSERT_METADATA) {
                                dataRecord.copy(meta = dataRecord.meta + dataRecordEvent.record.meta)
                            } else if(dataRecordEvent.command == DataRecordCommand.UPSERT_DOCUMENT_REPRESENTATION) {
                                dataRecord.copy(additionalRepresentations = dataRecord.additionalRepresentations + dataRecordEvent.record.additionalRepresentations)
                            } else {
                                throw Exception("Don't know how to handle $dataRecordEvent")
                            }

                        },
                        Materialized.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)))

        dataRecordTable.toStream().to(DATARECORD_CONSOLIDATED_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)))




        val topology = builder.build()

        println(topology.describe().toString())
        val streams = KafkaStreams(topology, streamsConfig)
        return streams
    }

/*
    private fun createMainStream(): KafkaStreams {
        val builder = StreamsBuilder()

        //have the documentrepresenation ingestion stream available
        val ingestionStream = builder.stream<Long, DocumentRepresentation>(DOCUMENTREPRESENTATION_INGESTION_TOPIC,
                Consumed.with(Serdes.LongSerde(),
                        KotlinSerde(DocumentRepresentation::class.java)))




        //collect & aggregate everything from  documentpresentation/ingestion  to a DataRecord
        val ingestionToDataRecordTable = ingestionStream
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

        //signal from docrep to metadata. This allows the MetadataProducers to work on metadata topic
        documentRepresentationStream.mapValues { value ->
            Metadata(mapOf("stored" to Date().toString(), "createdBy" to value.createdBy, "path" to value.path),
                    "docrep")
        }.to(METADATA_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(Metadata::class.java)))


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
        val metaToDataRecordAggregatorTable = builder.stream<Long, Metadata>(METADATA_TOPIC,
                Consumed.with(Serdes.LongSerde(),
                        KotlinSerde(Metadata::class.java)))
                .groupByKey()
                .aggregate(
                        { DataRecord() },
                        { key, metadata, dataRecord ->
                            dataRecord.copy(meta = dataRecord.meta + metadata)
                        },
                        Materialized.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)))


        val ingestionDataRecordJoinTable = metaToDataRecordAggregatorTable.join(ingestionToDataRecordTable,
                        { thisDataRecord, otherDataRecord ->
                            thisDataRecord.copy(representation = otherDataRecord.representation)
                        })
                .toStream().to(DATARECORD_EVENT_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)))

        val representationsDataRecordJoinTable = metaToDataRecordAggregatorTable.leftJoin(documentRepresentationDataRecordTable,
                        { meta, document ->
                            if(document != null)  {
                                meta.copy(additionalRepresentations = document.additionalRepresentations)
                            } else {
                                meta
                            }
                        })
        representationsDataRecordJoinTable.toStream().to(DATARECORD_EVENT_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)))

        val topology = builder.build()

        println(topology.describe().toString())
        val streams = KafkaStreams(topology, streamsConfig)
        return streams
    }
*/
}


