package integrationtests

import datatypes.Chunk
import datatypes.DataRecord
import datatypes.DataRecordWithChunks
import datatypes.DocumentRepresentation
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.JSON
import kotlinx.serialization.stringify
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import participants.*
import participants.file.*
import pipeline.impl.KafkaIntelligencePipeline
import pipeline.impl.KafkaIntelligencePipeline.Companion.CHUNK_TOPIC
import pipeline.impl.KafkaIntelligencePipeline.Companion.DATARECORD_CONSOLIDATED_TOPIC
import pipeline.impl.KafkaIntelligencePipeline.Companion.DATARECORD_EVENT_TOPIC
import pipeline.impl.KafkaIntelligencePipeline.Companion.DOCUMENTREPRESENTATION_EVENT_TOPIC
import pipeline.impl.KafkaIntelligencePipeline.Companion.DOCUMENTREPRESENTATION_INGESTION_TOPIC
import pipeline.impl.KafkaIntelligencePipeline.Companion.METADATA_EVENT_TOPIC
import pipeline.serialize.KotlinSerde
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

@ImplicitReflectionSerializer
class KafkaIntelligencePipelineTests {
    companion object {
        val embeddedMode = true
        fun deleteDir(file: File) {
            val contents = file.listFiles()
            if (contents != null) {
                for (f in contents) {
                    deleteDir(f)
                }
            }
            file.delete()
        }

        val cluster: EmbeddedKafkaCluster = EmbeddedKafkaCluster(1)
        val streamsConfig = Properties()
        val baseDir = File(".").absolutePath

        val testDir = "$baseDir/out/test"
        val stateDir = "$baseDir/out/state"
        var hostUrl = "localhost:8080"

        class DataRecordSerde() : KotlinSerde<datatypes.DataRecord>(datatypes.DataRecord::class.java)

        fun createPipeline(name: String, ingestors: List<PipelineIngestor>, producers: List<MetadataProducer>): KafkaIntelligencePipeline {
            val pipeline = KafkaIntelligencePipeline(hostUrl, stateDir, "testPipeline")
            ingestors.forEach { ingestor -> pipeline.registerIngestor(ingestor) }
            producers.forEach { producer -> pipeline.registerMetadataProducer(producer) }
//            pipeline.registerSideEffect("printer", {key, value -> println("$key: $value")  } )
            pipeline.registerSideEffect("filewriter") { key, value ->
                fileRepresentationStrategy(testDir, value, "json", true)?.bufferedWriter().use { out -> out?.write(JSON(indented = true).stringify(value)) }
            }

            pipeline.registry.register(FileOriginalContentCapability())

            pipeline.registry.register(FileTxtOutputProvider(testDir))
            pipeline.registry.register(FileTxtStringProvider(testDir))
            pipeline.registry.register(FileSimpleTextOutPathCapability(testDir))

            pipeline.registry.register(FileHtmlOutputProvider(testDir))
            pipeline.registry.register(FileHtmlStringProvider(testDir))
            pipeline.registry.register(FileHtmlTextOutPathCapability(testDir))

            pipeline.registerDocumentRepresentationProducer(TikaTxtDocumentRepresentationProducer(pipeline.registry))
            pipeline.registerDocumentRepresentationProducer(TikaHtmlDocumentRepresentationProducer(pipeline.registry))

            pipeline.registerChunkProducer("sentenceProducer", StanfordNlpSentenceChunkProducer(pipeline.registry))


            return pipeline
        }

        @AfterClass
        @JvmStatic
        fun shutdown() {
            Runtime.getRuntime().addShutdownHook(object : Thread() {
                override fun run() {
                    try {
                        println("cleaning up this mess...")
                        //pipeline?.stop()
                        deleteDir(File(stateDir))
                        if (embeddedMode) {
                            //dangerous!!!
                            val tempDir = File(System.getProperty("java.io.tmpdir"))

                            tempDir.listFiles().filter { file ->
                                file.name.startsWith("kafka") || file.name.startsWith("junit") || file.name.startsWith("librocksdbjni")
                            }
                                    .forEach { file ->
                                        println("deleting " + file.absolutePath)
                                        deleteDir(file)
                                    }
                            //doesn't work, at least on windows

                            //cluster.waitForRemainingTopics(1000)
                            //cluster.deleteTopicsAndWait(1000, *arrayOf(DOCUMENTREPRESENTATION_TOPIC, DOCUMENTREPRESENTATION_INGESTION_TOPIC, METADATA_TOPIC, DATARECORD_EVENT_TOPIC))
                        }
                        println("done, kthxbye")
                    } catch (t: Throwable) {
                        println("Was not able to delete topics " + t)
                    }
                }
            })
        }

        @BeforeClass
        @JvmStatic
        fun startup() {
            //embedded instance
            if (Files.exists(Paths.get(testDir))) {
                deleteDir(File(testDir))
            }
            Files.createDirectories(Paths.get(testDir))
            Files.createDirectories(Paths.get(stateDir))
            if (embeddedMode) {
                /*
                cluster.createTopic(DOCUMENTREPRESENTATION_INGESTION_TOPIC)
                cluster.createTopic(DOCUMENTREPRESENTATION_TOPIC)
                cluster.createTopic(METADATA_TOPIC)
                cluster.createTopic(DATARECORD_EVENT_TOPIC)
                 */
                cluster.start()
                cluster.deleteAndRecreateTopics(DOCUMENTREPRESENTATION_INGESTION_TOPIC, DOCUMENTREPRESENTATION_EVENT_TOPIC, METADATA_EVENT_TOPIC, DATARECORD_EVENT_TOPIC, CHUNK_TOPIC, DATARECORD_CONSOLIDATED_TOPIC)
                hostUrl = cluster.bootstrapServers()
                //pipeline = KafkaIntelligencePipeline(cluster.bootstrapServers(), stateDir)
                println("starting embedded kafka cluster with zookeeper ${cluster.zKConnectString()} and bootstrapServes ${cluster.bootstrapServers()}")
                streamsConfig.put("bootstrap.servers", cluster.bootstrapServers())

            } else {
                hostUrl = "localhost:29092"
                println("starting with running kafka cluster at " + hostUrl)
                //pipeline = KafkaIntelligencePipeline(hostUrl, stateDir)
                streamsConfig.put("bootstrap.servers", hostUrl)

            }


            //running instance
            streamsConfig.put("auto.offset.reset", "earliest")
            // TODO("correct config for state.dir")
            streamsConfig.put("state.dir", Files.createTempDirectory("kafka").toAbsolutePath().toString())
            streamsConfig.put("default.key.serde", Serdes.Long().javaClass)
            streamsConfig.put("default.value.serde", Serdes.ByteArray().javaClass)
            //streamsConfig.put("default.value.serde", DataRecordSerde())
            streamsConfig.put("cache.max.bytes.buffering", 0)
            streamsConfig.put("internal.leave.group.on.close", true)
            streamsConfig.put("commit.interval.ms", 100)
            streamsConfig.put("application.id", "KafkaIntelligencePipelineTests")
            streamsConfig.put("delete.topic.enable", "true")


        }

    }

    @Test
    @Throws(Exception::class)
    fun testRogueMetadataProducer() {
        println("baseDir is $baseDir")

        class RogueMetadataProducer() : MetadataProducer {
            override val name = "rogue";
            suspend override fun produce(record: datatypes.DataRecord): datatypes.Metadata {
                throw RuntimeException("na, won't to!")
            }
        }

        val name = "testRogueMetadataProducer"
        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("$baseDir/pipeline-kafka/src/test/resources/testresources")), emptyList<MetadataProducer>())

        pipeline.registerMetadataProducer(TikaMetadataProducer(pipeline.registry))
        pipeline.registerMetadataProducer(RogueMetadataProducer())

        val view = runPipeline(pipeline, { kv -> kv.meta.any { metadata -> metadata.createdBy == TikaMetadataProducer(pipeline.registry).name } }, 3)
        pipeline.stop()
    }

    @Ignore
    @Throws(Exception::class)
    fun testLargerDirectory() {
        val name = "testLargeDirectory"

        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("c:\\Users\\Christian\\Documents\\Architecture")), emptyList())

        pipeline.registerMetadataProducer(HashMetadataProducer())
        val tikaMetadataProducer = TikaMetadataProducer(pipeline.registry)
        pipeline.registerMetadataProducer(tikaMetadataProducer)


        runPipeline(pipeline, { record: datatypes.DataRecord -> true }, 2000)
        pipeline.stop()
    }


    @Test
    @Throws(Exception::class)
    fun testHashMetadataProducer() {
        val name = "testHashMetadataProducer"

        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("$baseDir/pipeline-kafka/src/test/resources/testresources")), emptyList())

        pipeline.registerMetadataProducer(HashMetadataProducer())

        val view = runPipeline(pipeline, { kv: datatypes.DataRecord ->
            kv.meta.any { metadata -> metadata.createdBy == HashMetadataProducer().name }
        }, 3)
        pipeline.stop()
    }

    @Test
    @Throws(Exception::class)
    fun testStanfordNlpParser() {
        val name = "testStanfordNlpParser"

        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("$baseDir/pipeline-kafka/src/test/resources/testresources")), emptyList<MetadataProducer>())

        val nlpParserProducer = StanfordNlpParserProducer(pipeline.registry)
        val tikaMetadataProducer = TikaMetadataProducer(pipeline.registry)
        pipeline.registerMetadataProducer(nlpParserProducer)
        pipeline.registerMetadataProducer(tikaMetadataProducer)

        val view = runPipeline(pipeline, { kv -> kv.meta.any { metadata -> metadata.createdBy == nlpParserProducer.name } }, 3)
        pipeline.stop()
    }

    @Test
    @Throws(Exception::class)
    fun testDirectoryCrawlAndTikaChunkLanguageDetection() {
        val name = "testDirectoryCrawlAndTikaChunkLanguageDetection"
        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("$baseDir/pipeline-kafka/src/test/resources/testresources")), emptyList<MetadataProducer>())

        pipeline.registerChunkMetadataProducer(TikaChunkLanguageDetection())

        val builder = StreamsBuilder()
        val myStreamConfig = Properties()
        myStreamConfig.putAll(streamsConfig)
        myStreamConfig.put("application.id", "stream_" + name)

        val chunkStream =
                builder.stream<Long, Chunk>(CHUNK_TOPIC,
                        Consumed.with(Serdes.LongSerde(),
                                KotlinSerde(Chunk::class.java)))
        chunkStream.foreach { k, v -> println("chunk --- $k: $v") }

        val dataRecordStream =
                builder.stream<Long, DataRecord>(DATARECORD_CONSOLIDATED_TOPIC,
                        Consumed.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)))

        dataRecordStream.join(chunkStream, { dataRecord, chunk -> Pair(dataRecord, chunk) },
                JoinWindows.of(1000),
                Joined.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java), KotlinSerde(Chunk::class.java)))
                .foreach({ key, value -> println("joined  $key/${value.second.parentId}/${value.second.index}") })


        val streams = KafkaStreams(builder.build(), myStreamConfig)
        streams.cleanUp()
        streams.start()


        val view = runPipeline(pipeline, { kv -> kv.meta.any { metadata -> metadata.createdBy == TikaChunkLanguageDetection().name } }, 3)
        streams.close()
        pipeline.stop()
    }


    @Test
    @Throws(Exception::class)
    fun testChannels() {
        val name = "testChannels"
        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("$baseDir/pipeline-kafka/src/test/resources/testresources")), emptyList<MetadataProducer>())

        pipeline.registerChunkMetadataProducer(TikaChunkLanguageDetection())

        val builder = StreamsBuilder()
        val myStreamConfig = Properties()
        myStreamConfig.putAll(streamsConfig)
        myStreamConfig.put("application.id", "stream_" + name)

        val chunkStream =
                builder.stream<Long, Chunk>(CHUNK_TOPIC,
                        Consumed.with(Serdes.LongSerde(),
                                KotlinSerde(Chunk::class.java)))


        val channel = Channel<DataRecordWithChunks>(1)

        val dataRecordStream =
                builder.stream<Long, DataRecord>(DATARECORD_CONSOLIDATED_TOPIC,
                        Consumed.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java)))

        val joinedStream = dataRecordStream.join(chunkStream, { dataRecord, chunk -> Pair(dataRecord, chunk) },
                JoinWindows.of(1000),
                Joined.with(Serdes.LongSerde(), KotlinSerde(DataRecord::class.java), KotlinSerde(Chunk::class.java)))
/*
        val complete = joinedStream.groupByKey().aggregate({DataRecordWithChunks()},
                { key,newValue, aggregate ->
                   DataRecordWithChunks(dataRecord = newValue.first, chunks = aggregate.chunks + newValue.second)
                    },
                Materialized.with(Serdes.LongSerde(), KotlinSerde(DataRecordWithChunks::class.java)))
*/
        val complete = joinedStream.groupByKey().windowedBy(SessionWindows.with(500000))
                .aggregate({ DataRecordWithChunks() },
                        { key, newValue, aggregate ->
                            DataRecordWithChunks(dataRecord = newValue.first, chunks = aggregate.chunks + newValue.second)
                        },
                        { key, aggOne, aggTwo ->
                            DataRecordWithChunks(dataRecord = aggOne.dataRecord, chunks = aggOne.chunks + aggTwo.chunks)
                        },
                        Materialized.with(Serdes.LongSerde(), KotlinSerde(DataRecordWithChunks::class.java)))

        complete.toStream().foreach { key, value -> GlobalScope.async { channel.send(value) } }

        val streams = KafkaStreams(builder.build(), myStreamConfig)
        streams.cleanUp()
        streams.start()

        GlobalScope.async {
            channel.consumeEach { println("consume ${it.dataRecord.name}") }
        }

        runPipeline(pipeline, { kv -> kv.meta.any { metadata -> metadata.createdBy == TikaChunkLanguageDetection().name } }, 3)
        channel.close()
        streams.close()
        pipeline.stop()
    }


    @Test
    fun testDirectoryCrawlAndTika() {
        val name = "testDirectoryCrawlAndTika"
        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("$baseDir/pipeline-kafka/src/test/resources/testresources")), emptyList<MetadataProducer>())

        pipeline.registerMetadataProducer(TikaMetadataProducer(pipeline.registry))
        runPipeline(pipeline, { kv -> kv.meta.any { metadata -> metadata.createdBy == TikaMetadataProducer(pipeline.registry).name } }, 3)
        pipeline.stop()
    }

    @Test
    fun testChannelConsumption() {
        val name = "testDirectoryCrawlAndTika"
        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("$baseDir/pipeline-kafka/src/test/resources/testresources")), emptyList<MetadataProducer>())

        pipeline.registerMetadataProducer(TikaMetadataProducer(pipeline.registry))
        runPipeline(pipeline, { kv -> kv.meta.any { metadata -> metadata.createdBy == TikaMetadataProducer(pipeline.registry).name } }, 3)
        //the stream should have been consumed
        runPipeline(pipeline, { kv -> kv.meta.any { metadata -> metadata.createdBy == TikaMetadataProducer(pipeline.registry).name } }, 0)
        //but with a new id, the stream starts from beginning
        runPipeline(pipeline, { kv -> kv.meta.any { metadata -> metadata.createdBy == TikaMetadataProducer(pipeline.registry).name } }, expectedResults = 3, timeout = 10000, id = "2nd" )
        pipeline.stop()
    }

    fun runPipeline(pipeline: KafkaIntelligencePipeline,
                    predicate: (datatypes.DataRecord) -> Boolean,
                    expectedResults: Int,
                    timeout: Long = 60000L,
                    id: String = ""): List<datatypes.DataRecord> {
        val view = mutableListOf<DataRecord>()
        runBlocking {
            val job = GlobalScope.launch {
                pipeline.run()
            }
            job.join()
            withTimeout(timeout) {
                val dataRecords = pipeline.dataRecords(id)
                var i = 0
                while(i<expectedResults) {
                    val record = dataRecords.receive()
                    if(predicate(record)) {
                        view.add(record)
                        i++
                    }
                }
            }
            pipeline.stop()
//            view = createDataRecords(storeName)
        }
        return view
    }

    private suspend fun createDataRecords(name: String): List<datatypes.DataRecord> {
        val builder = StreamsBuilder()
        val table = builder.table<Long, datatypes.DataRecord>(DATARECORD_CONSOLIDATED_TOPIC,
                Consumed.with(Serdes.LongSerde(), DataRecordSerde()),
                Materialized.`as`(name))
        val myStreamConfig = Properties()
        myStreamConfig.putAll(streamsConfig)
        myStreamConfig.put("application.id", "stream_" + name)

        /*
        val chunkStream =
                builder.stream<Long, Chunk>(CHUNK_TOPIC,
                        Consumed.with(Serdes.LongSerde(),
                                KotlinSerde(Chunk::class.java)))
        chunkStream.foreach{k,v -> println("chunk --- $k: $v")}
        */

        val streams = KafkaStreams(builder.build(), myStreamConfig)
        streams.cleanUp()
        streams.start()

        delay(8000)
        val store = streams.store(table.queryableStoreName(), QueryableStoreTypes.keyValueStore<Long, datatypes.DataRecord>())


        val view = store.all().asSequence().toList()

        streams.close()
        return view.map { keyValue -> keyValue.value }
    }
}

private class TestIngestor(val content:List<String>) :PipelineIngestor {
    override suspend fun ingest(channel: SendChannel<DocumentRepresentation>) {
        content.forEach {
            channel.send(datatypes.DocumentRepresentation(it, this.name))
        }
    }

    val name = "test"

}