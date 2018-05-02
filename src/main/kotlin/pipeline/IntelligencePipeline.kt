package pipeline

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import datatypes.*
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.delay
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
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import participants.*
import pipeline.capabilities.Capability
import pipeline.capabilities.DefaultCapabilityRegistry
import pipeline.serialize.KotlinSerde
import pipeline.serialize.serialize
import util.log
import java.util.*
import com.google.common.collect.MinMaxPriorityQueue.maximumSize
import kotlinx.coroutines.experimental.launch
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder
import java.util.concurrent.TimeUnit


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
    lateinit var dataRecordTable:KTable<Long,DataRecord>

    val subStreams = mutableListOf<KafkaStreams>()

    val ingestors = mutableListOf<PipelineIngestor>()
    val ingestionChannel = Channel<DocumentRepresentation>(Int.MAX_VALUE)

    /*
    var cache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(10, TimeUnit.SECONDS)
            .build<Long,DataRecord>()

    private val cacheSideEffect:PipelineSideEffect =  {
        key, record -> cache.put(key,record)
    }
*/
    fun all():List<DataRecord> {
        val iter =streams?.store(dataRecordTable.queryableStoreName(), QueryableStoreTypes.keyValueStore<Long, DataRecord>())
                ?.all()
        val res = iter?.asSequence()?.toList()?.map { keyValue ->  keyValue.value}?: emptyList()
        iter?.close()
        return res
    }
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
                .filter { _, value ->  value != null}
                .flatMap { key,value: DataRecord -> runBlocking { chunkProducer.chunks(value,key) }
                        .mapNotNull { valueRes -> KeyValue<Long,Chunk>(key,valueRes)  } .asIterable() }
                .mapValues { kv -> kv }
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
        datarecordStream.filter { _, value ->  value!=null }.foreach { key, value ->  sideEffect(key, value)}


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
                .filter { key, value ->
                    value != null &&
                    !value.meta.any { metadata ->
                    metadata.createdBy == prod.name }
                }
                .mapValues { value ->
                    MetadataEvent(BaseCommand.UPSERT,prod.metadataFor(value))
/*
                    val job= async {
                        MetadataEvent(BaseCommand.UPSERT,prod.metadataFor(value))

                    }
                    runBlocking { job.await() }
*/
                }.filter { _, value ->
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
                .filter { _, value -> value != null && !value.additionalRepresentations.any { representation -> representation.createdBy == prod.name } }
                //TODO: filter out the those that are exactly the same as before!
                .mapValues { datarecord ->
                    prod.documentRepresentationFor(datarecord)
                }.filter { _, value ->
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
        launch {
            //doesn't work: somehow produces null values, and I don't know why...

            //  registerSideEffect("cache", cacheSideEffect)

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

        val consolidatedDataRecordTable = dataRecordEventStream
                .groupByKey()
                .aggregate(
                        { DataRecord() },
                        { _, dataRecordEvent, dataRecord ->
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

        consolidatedDataRecordTable
                //doesn't work: somehow produces null values, and I don't know why...
                /*
                .filter { k,v-> v != null}
                .filterNot { key, value ->
            cache.getIfPresent(key)?.equals(value)?:false
                }
        .mapValues { key, value -> println(">>$key / $value"); value}
                */

        .toStream().to(DATARECORD_CONSOLIDATED_TOPIC, Produced.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)))

        dataRecordTable = builder.table<Long, DataRecord>(DATARECORD_CONSOLIDATED_TOPIC,
                Consumed.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)),
                Materialized.`as`("dataRecords"))


        val topology = builder.build()

        println(topology.describe().toString())
        val streams = KafkaStreams(topology, streamsConfig)

        return streams
    }

}


