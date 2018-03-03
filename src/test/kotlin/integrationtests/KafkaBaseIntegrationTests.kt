package integrationtests

import org.apache.kafka.common.serialization.*
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.test.IntegrationTest
import org.apache.kafka.test.TestUtils
import org.junit.*
import org.junit.experimental.categories.Category
import java.util.*

@Category(IntegrationTest::class)
class KafkaBaseIntegrationTests {
    private val INPUT_TOPIC_1 = "inputTopicLeft"
    private val INPUT_TOPIC_2 = "inputTopicRight"
    private val OUTPUT_TOPIC = "outputTopic"

    companion object {
        @get:ClassRule
        @JvmStatic
        val CLUSTER = EmbeddedKafkaCluster(1)
        val PRODUCER_CONFIG = Properties()
        val RESULT_CONSUMER_CONFIG = Properties()
        val STREAMS_CONFIG = Properties()

        @BeforeClass  @JvmStatic
        fun setupConfigsAndUtils() {
            PRODUCER_CONFIG.put("bootstrap.servers", CLUSTER.bootstrapServers())
            PRODUCER_CONFIG.put("acks", "all")
            PRODUCER_CONFIG.put("retries", 0)
            PRODUCER_CONFIG.put("key.serializer", LongSerializer::class.java)
            PRODUCER_CONFIG.put("value.serializer", StringSerializer::class.java)
            RESULT_CONSUMER_CONFIG.put("bootstrap.servers", CLUSTER.bootstrapServers())
            RESULT_CONSUMER_CONFIG.put("group.id", "join-integration-test-result-consumer")
            RESULT_CONSUMER_CONFIG.put("auto.offset.reset", "earliest")
            RESULT_CONSUMER_CONFIG.put("key.deserializer", LongDeserializer::class.java)
            RESULT_CONSUMER_CONFIG.put("value.deserializer", StringDeserializer::class.java)
            STREAMS_CONFIG.put("bootstrap.servers", CLUSTER.bootstrapServers())
            STREAMS_CONFIG.put("auto.offset.reset", "earliest")
            STREAMS_CONFIG.put("state.dir", TestUtils.tempDirectory().path)
            STREAMS_CONFIG.put("default.key.serde", Serdes.Long().javaClass)
            STREAMS_CONFIG.put("default.value.serde", Serdes.String().javaClass)
            STREAMS_CONFIG.put("cache.max.bytes.buffering", 0)
            STREAMS_CONFIG.put("internal.leave.group.on.close", true)
            STREAMS_CONFIG.put("commit.interval.ms", 100)
        }
    }

    @After
    @Throws(InterruptedException::class)
    fun cleanup() {
      CLUSTER.deleteTopicsAndWait(1000, *arrayOf(INPUT_TOPIC_1,INPUT_TOPIC_2,OUTPUT_TOPIC))
    }



    @Test
    @Throws(Exception::class)
    fun testSimpleStream() {
        val builder = StreamsBuilder()
        val leftStream = builder.stream<Long, String>(INPUT_TOPIC_1)
        val out = leftStream.filter {k,v -> k.toInt()%3==0 }.foreach {k,v-> println(v)}//.to(INPUT_TOPIC_1)


        STREAMS_CONFIG.put("application.id", "testSimpleStream")


        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG)
        val streams = KafkaStreams(builder.build(), STREAMS_CONFIG)
        streams.start()

        val kv = mutableListOf<KeyValue<Long,String>>()
        (0..100).forEach { key -> kv.add(KeyValue(key.toLong(),""+key))}

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_1,kv, PRODUCER_CONFIG,10000)


    }


}