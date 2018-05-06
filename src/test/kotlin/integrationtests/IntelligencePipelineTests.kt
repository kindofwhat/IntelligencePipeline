package integrationtests

import datatypes.Chunk
import datatypes.DataRecord
import datatypes.Metadata
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeout
import kotlinx.serialization.json.JSON
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.jboss.netty.channel.Channels.future
import org.junit.*
import org.junit.Assert.assertEquals
import participants.file.*
import participants.*
import pipeline.IntelligencePipeline
import pipeline.IntelligencePipeline.Companion.CHUNK_TOPIC
import pipeline.IntelligencePipeline.Companion.DATARECORD_CONSOLIDATED_TOPIC
import pipeline.IntelligencePipeline.Companion.METADATA_EVENT_TOPIC
import pipeline.IntelligencePipeline.Companion.DOCUMENTREPRESENTATION_INGESTION_TOPIC
import pipeline.IntelligencePipeline.Companion.DATARECORD_EVENT_TOPIC
import pipeline.IntelligencePipeline.Companion.DOCUMENTREPRESENTATION_EVENT_TOPIC
import pipeline.serialize.KotlinSerde
import java.io.File
import java.nio.file.Files
import java.util.*


class IntelligencePipelineTests {



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
        val cluster:EmbeddedKafkaCluster = EmbeddedKafkaCluster(1)
        val streamsConfig = Properties()
        val stateDir = "build/test/state"
        var hostUrl = "localhost:8080"

        class DataRecordSerde() : KotlinSerde<DataRecord>(DataRecord::class.java)

        fun createPipeline(name:String, ingestors: List<PipelineIngestor>, producers:List<MetadataProducer>): IntelligencePipeline {
            val pipeline = IntelligencePipeline(hostUrl, stateDir,"testPipeline")
            ingestors.forEach { ingestor -> pipeline.registerIngestor(ingestor)}
            producers.forEach { producer -> pipeline.registerMetadataProducer(producer)}
//            pipeline.registerSideEffect("printer", {key, value -> println("$key: $value")  } )
            pipeline.registerSideEffect("filewriter", {key, value ->
                fileRepresentationStrategy("out/test",value,"json", true)?.bufferedWriter().use { out -> out?.write(JSON(indented = true).stringify(value)) }
            } )

            pipeline.registry.register(FileOriginalContentCapability())

            pipeline.registry.register(FileTxtOutputProvider("out/test"))
            pipeline.registry.register(FileTxtStringProvider("out/test"))
            pipeline.registry.register(FileSimpleTextOutPathCapability("out/test"))

            pipeline.registry.register(FileHtmlOutputProvider("out/test"))
            pipeline.registry.register(FileHtmlStringProvider("out/test"))
            pipeline.registry.register(FileHtmlTextOutPathCapability("out/test"))

            pipeline.registerDocumentRepresentationProducer(TikaTxtDocumentRepresentationProducer(pipeline.registry))
            pipeline.registerDocumentRepresentationProducer(TikaHtmlDocumentRepresentationProducer(pipeline.registry))

            pipeline.registerChunkProducer("sentenceProducer", StanfordNlpSentenceChunkProducer(pipeline.registry))


            return pipeline
        }

        @AfterClass @JvmStatic
        fun shutdown() {
            Runtime.getRuntime().addShutdownHook(object : Thread() {
                override fun run() {
                    try {
                        println("cleaning up this mess...")
                        //pipeline?.stop()
                        deleteDir(File(stateDir))
                        if(embeddedMode) {
                            //dangerous!!!
                            val tempDir = File(System.getProperty("java.io.tmpdir"))

                            tempDir.listFiles().filter { file ->
                                file.name.startsWith("kafka") || file.name.startsWith("junit") || file.name.startsWith("librocksdbjni") }
                                    .forEach { file ->
                                        println("deleting " + file.absolutePath)
                                        deleteDir(file)
                                    }
                            //doesn't work, at least on windows

                            //cluster.waitForRemainingTopics(1000)
                            //cluster.deleteTopicsAndWait(1000, *arrayOf(DOCUMENTREPRESENTATION_TOPIC, DOCUMENTREPRESENTATION_INGESTION_TOPIC, METADATA_TOPIC, DATARECORD_EVENT_TOPIC))
                        }
                        println("done, kthxbye")
                    }catch (t:Throwable)  {
                        println("Was not able to delete topics " + t)
                    }
                }
            })
        }

        @BeforeClass @JvmStatic
        fun startup() {
            //embedded instance
            deleteDir(File("out/test"))
            if(embeddedMode) {
                /*
                cluster.createTopic(DOCUMENTREPRESENTATION_INGESTION_TOPIC)
                cluster.createTopic(DOCUMENTREPRESENTATION_TOPIC)
                cluster.createTopic(METADATA_TOPIC)
                cluster.createTopic(DATARECORD_EVENT_TOPIC)
                 */
                cluster.start()
                cluster.deleteAndRecreateTopics(DOCUMENTREPRESENTATION_INGESTION_TOPIC,DOCUMENTREPRESENTATION_EVENT_TOPIC,METADATA_EVENT_TOPIC,DATARECORD_EVENT_TOPIC, CHUNK_TOPIC, DATARECORD_CONSOLIDATED_TOPIC)
                hostUrl = cluster.bootstrapServers()
                //pipeline = IntelligencePipeline(cluster.bootstrapServers(), stateDir)
                println("starting embedded kafka cluster with zookeeper ${cluster.zKConnectString()} and bootstrapServes ${cluster.bootstrapServers()}" )
                streamsConfig.put("bootstrap.servers", cluster.bootstrapServers())

            } else {
                hostUrl = "liu:9092"
                println("starting with running kafka cluster at " + hostUrl)
                //pipeline = IntelligencePipeline(hostUrl, stateDir)
                streamsConfig.put("bootstrap.servers", hostUrl)

            }


            //running instance
            /*
            pipeline = IntelligencePipeline("liu:9092")
            streamsConfig.put("bootstrap.servers", "liu:9092")
            */
            streamsConfig.put("auto.offset.reset", "earliest")
            // TODO("correct config for state.dir")
            streamsConfig.put("state.dir", Files.createTempDirectory("kafka").toAbsolutePath().toString())
            streamsConfig.put("default.key.serde", Serdes.Long().javaClass)
            streamsConfig.put("default.value.serde", Serdes.ByteArray().javaClass)
            //streamsConfig.put("default.value.serde", DataRecordSerde())
            streamsConfig.put("cache.max.bytes.buffering", 0)
            streamsConfig.put("internal.leave.group.on.close", true)
            streamsConfig.put("commit.interval.ms", 100)
            streamsConfig.put("application.id", "IntelligencePipelineTests")
            streamsConfig.put("delete.topic.enable", "true")


        }

    }

    @Test
    @Throws(Exception::class)
    fun testRogueMetadataProducer() {
        class RogueMetadataProducer() :MetadataProducer {
            override val name="rogue";
            override fun metadataFor(record: DataRecord): Metadata {
                throw RuntimeException("na, won't to!")
            }
        }

        val name = "testRogueMetadataProducer"
        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("src/test/resources/testresources")), emptyList<MetadataProducer>())

        pipeline.registerMetadataProducer(TikaMetadataProducer(pipeline.registry))
        pipeline.registerMetadataProducer(RogueMetadataProducer())

        val view = runPipeline(pipeline,{ kv -> kv.meta.any { metadata ->  metadata.createdBy == TikaMetadataProducer(pipeline.registry).name} }, 3)
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


        runPipeline(pipeline,{record:DataRecord -> true}, 2000)
        pipeline.stop()
    }


    @Test
    @Throws(Exception::class)
    fun testHashMetadataProducer() {
        val name = "testHashMetadataProducer"

        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("src/test/resources/testresources")), emptyList())

        pipeline.registerMetadataProducer(HashMetadataProducer())

        val view = runPipeline(pipeline,  {
            kv:DataRecord ->
                kv.meta.any { metadata ->  metadata.createdBy == HashMetadataProducer().name}},3)
        pipeline.stop()
    }

    @Test
    @Throws(Exception::class)
    fun testStanfordNlpParser() {
        val name = "testStanfordNlpParser"

        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("src/test/resources/testresources")), emptyList<MetadataProducer>())

        val nlpParserProducer = StanfordNlpParserProducer(pipeline.registry)
        val tikaMetadataProducer = TikaMetadataProducer(pipeline.registry)
        pipeline.registerMetadataProducer(nlpParserProducer)
        pipeline.registerMetadataProducer(tikaMetadataProducer)

        val view = runPipeline(pipeline,{ kv -> kv.meta.any { metadata ->  metadata.createdBy == nlpParserProducer.name} },3)
        pipeline.stop()
    }

    @Test
    @Throws(Exception::class)
    fun testDirectoryCrawlAndTikaChunkLanguageDetection() {
        val name = "testDirectoryCrawlAndTikaChunkLanguageDetection"
        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("src/test/resources/testresources")), emptyList<MetadataProducer>())

        pipeline.registerChunkMetadataProducer(TikaChunkLanguageDetection())


        val view = runPipeline(pipeline,{ kv -> kv.meta.any { metadata ->  metadata.createdBy == TikaChunkLanguageDetection().name} },3)
        pipeline.stop()
    }

    @Test
    @Throws(Exception::class)
    fun testDirectoryCrawlAndTika() {
        val name = "testDirectoryCrawlAndTika"
        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("src/test/resources/testresources")), emptyList<MetadataProducer>())

        pipeline.registerMetadataProducer(TikaMetadataProducer(pipeline.registry))


        val view = runPipeline(pipeline,{ kv -> kv.meta.any { metadata ->  metadata.createdBy == TikaMetadataProducer(pipeline.registry).name} },3)
        pipeline.stop()
    }


     fun runPipeline(pipeline: IntelligencePipeline,
                     predicate: (DataRecord) -> Boolean,
                     expectedResults:Int,
                     timeout:Long = 40000L): List<DataRecord> {
        var view = emptyList<DataRecord>()
        runBlocking {
            val job = launch {
                pipeline.run()
            }
            job.join()
            delay(2000)
            withTimeout(timeout) {
                repeat@while(true) {
                    delay(500L)
                    view = pipeline.all().filter(predicate)
                    if(view.size >= expectedResults) {
                        break@repeat
                    }
                }
            }
//            view = createDataRecords(storeName)
        }
        return view
    }

    private suspend fun createDataRecords(name: String): List<DataRecord> {
        val builder = StreamsBuilder()
        val table = builder.table<Long, DataRecord>(DATARECORD_CONSOLIDATED_TOPIC,
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
        val store = streams.store(table.queryableStoreName(), QueryableStoreTypes.keyValueStore<Long, DataRecord>())


        val view = store.all().asSequence().toList()

        streams.close()
        return view.map { keyValue ->  keyValue.value}
    }
}