package integrationtests

import datatypes.DataRecord
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.junit.*
import org.junit.Assert.assertEquals
import participants.*
import pipeline.IntelligencePipeline
import pipeline.IntelligencePipeline.Companion.METADATA_TOPIC
import pipeline.IntelligencePipeline.Companion.DOCUMENTREPRESENTATION_INGESTION_TOPIC
import pipeline.IntelligencePipeline.Companion.DATARECORD_TOPIC
import pipeline.IntelligencePipeline.Companion.DOCUMENTREPRESENTATION_TOPIC
import pipeline.serialize.KotlinSerde
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
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
            val pipeline = IntelligencePipeline(hostUrl, stateDir,name)
            ingestors.forEach { ingestor -> pipeline.registerIngestor(ingestor)}
            producers.forEach { producer -> pipeline.registerMetadataProducer(producer)}
            return pipeline
        }

        @AfterClass @JvmStatic
        fun shutdown() {
            val mainThread = Thread.currentThread()
            Runtime.getRuntime().addShutdownHook(object : Thread() {
                override fun run() {
                    try {
                        println("cleaning up this mess...")
                        //pipeline?.stop()
                        deleteDir(File(stateDir))
                        if(embeddedMode) {
                            cluster.waitForRemainingTopics(1000)
                            cluster.deleteTopicsAndWait(1000, *arrayOf(DOCUMENTREPRESENTATION_TOPIC, DOCUMENTREPRESENTATION_INGESTION_TOPIC, METADATA_TOPIC, DATARECORD_TOPIC))
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
            if(embeddedMode) {
                /*
                cluster.createTopic(DOCUMENTREPRESENTATION_INGESTION_TOPIC)
                cluster.createTopic(DOCUMENTREPRESENTATION_TOPIC)
                cluster.createTopic(METADATA_TOPIC)
                cluster.createTopic(DATARECORD_TOPIC)
                 */
                cluster.start()
                cluster.deleteAndRecreateTopics(DOCUMENTREPRESENTATION_INGESTION_TOPIC,DOCUMENTREPRESENTATION_TOPIC,METADATA_TOPIC,DATARECORD_TOPIC)
                hostUrl = cluster.bootstrapServers()
                //pipeline = IntelligencePipeline(cluster.bootstrapServers(), stateDir)
                println("starting embedded kafka cluster with " + cluster.bootstrapServers())
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
    fun testDirectoryCrawlAndHashCreation() {
        val view = createPipelineAndRunWithResults("testDirectoryCrawlAndHashCreation",
                listOf(DirectoryIngestor("src/test/resources")),
                listOf(HashMetadataProducer()))
        println(view)
        assertEquals(4, view.size)
        assertEquals(3, view.filter { kv -> kv.meta.any { metadata ->  metadata.createdBy == HashMetadataProducer().name} }.size)
    }

    @Test
    @Throws(Exception::class)
    fun testDirectoryCrawlAndTika() {
        val view = createPipelineAndRunWithResults("testDirectoryCrawlAndTika",
                listOf(DirectoryIngestor("src/test/resources")),
                listOf(TikaMetadataProducer()))
        assertEquals(4, view.size)
        assertEquals(3, view.filter { kv -> kv.meta.any { metadata ->  metadata.createdBy == TikaMetadataProducer().name} }.size)
    }




    private fun createPipelineAndRunWithResults(name:String, ingestors: List<PipelineIngestor>, producers:List<MetadataProducer>): List<DataRecord> {
        val pipeline = createPipeline(name,ingestors,producers)
        val view = runPipeline(pipeline)
        return view
    }


    private fun runPipeline(pipeline: IntelligencePipeline): List<DataRecord> {
        var view = emptyList<DataRecord>()
        launch {
            pipeline.run()
        }
        runBlocking {
            view = createDataRecords("testDirectoryCrawlAndTika")
        }
        return view
    }

    private suspend fun createDataRecords(storeName: String): List<DataRecord> {
        val builder = StreamsBuilder()
        val table = builder.table<Long, DataRecord>(DATARECORD_TOPIC,
                Consumed.with(Serdes.LongSerde(), DataRecordSerde()),
                Materialized.`as`(storeName))
        val streams = KafkaStreams(builder.build(), streamsConfig)
        streams.start()
        delay(4000)
        val store = streams.store(table.queryableStoreName(), QueryableStoreTypes.keyValueStore<Long, DataRecord>())
        val view = store.all().asSequence().toList()
        streams.close()
        delay(1000)
        streams.cleanUp()
        return view.map { keyValue ->  keyValue.value}
    }
}