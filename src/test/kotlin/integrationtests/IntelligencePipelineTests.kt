package integrationtests

import datatypes.DataRecord
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster
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
        val embeddedMode = false
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
        var pipeline:IntelligencePipeline? = null
        class DataRecordSerde() : KotlinSerde<DataRecord>(DataRecord::class.java)

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
                pipeline = IntelligencePipeline(cluster.bootstrapServers(), stateDir)
                println("starting embedded kafka cluster with " + cluster.bootstrapServers())
                streamsConfig.put("bootstrap.servers", cluster.bootstrapServers())

            } else {
                val hostUrl = "liu:9092"
                println("starting with running kafka cluster at " + hostUrl)
                pipeline = IntelligencePipeline(hostUrl, stateDir)
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
        pipeline?.registerIngestor(DirectoryIngestor("src/test/resources"))
        pipeline?.registerMetadataProducer(TikaMetadataProducer())
        pipeline?.registerMetadataProducer(HashMetadataProducer())
        pipeline?.registerMetadataProducer(AzureCognitiveServicesMetadataProducer("https://westus.api.cognitive.microsoft.com/text/analytics/v2.0/keyPhrases", System
                .getProperty("accessKey"), pipeline?.registry!!))
        //streamsConfig.put("processing.guarantee", "exactly_once")
        val builder = StreamsBuilder()
        val table = builder.table<Long,DataRecord>(IntelligencePipeline.DATARECORD_TOPIC,
                Consumed.with(Serdes.LongSerde(),DataRecordSerde()),
                Materialized.`as`("DataRecordStore"))
        val streams = KafkaStreams(builder.build(), streamsConfig)

        launch {
            pipeline?.run()
        }
        runBlocking {
            delay(2000)
            println("Starting test stream")
            streams.start()
            delay(6000)
            println("retrieving view")
            val view = streams.store(table.queryableStoreName(), QueryableStoreTypes.keyValueStore<Long, DataRecord>())
                    .all().asSequence().toList()
            println(view)
            assertEquals(4, view.size)
            assertEquals(3, view.filter { kv -> kv.value.meta.any { metadata ->  metadata.createdBy == HashMetadataProducer().name} }.size)
            assertEquals(3, view.filter { kv -> kv.value.meta.any { metadata ->  metadata.createdBy == TikaMetadataProducer().name} }.size)
            streams.close()
        }
    }
}