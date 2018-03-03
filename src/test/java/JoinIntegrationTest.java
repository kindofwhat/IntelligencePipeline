//
// Source name recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//


import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class})
public class JoinIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    private static final String APP_ID = "join-integration-test";
    private static final String INPUT_TOPIC_1 = "inputTopicLeft";
    private static final String INPUT_TOPIC_2 = "inputTopicRight";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static final Properties PRODUCER_CONFIG = new Properties();
    private static final Properties RESULT_CONSUMER_CONFIG = new Properties();
    private static final Properties STREAMS_CONFIG = new Properties();
    private StreamsBuilder builder;
    private KStream<Long, String> leftStream;
    private KStream<Long, String> rightStream;
    private KTable<Long, String> leftTable;
    private KTable<Long, String> rightTable;
    private final List<JoinIntegrationTest.Input<String>> input = Arrays.asList(new JoinIntegrationTest.Input("inputTopicLeft", (String)null),
            new JoinIntegrationTest.Input("inputTopicRight", (String)null), new JoinIntegrationTest.Input("inputTopicLeft", "A"), new JoinIntegrationTest.Input("inputTopicRight", "a"), new JoinIntegrationTest.Input("inputTopicLeft", "B"), new JoinIntegrationTest.Input("inputTopicRight", "b"), new JoinIntegrationTest.Input("inputTopicLeft", (String)null), new JoinIntegrationTest.Input("inputTopicRight", (String)null), new JoinIntegrationTest.Input("inputTopicLeft", "C"), new JoinIntegrationTest.Input("inputTopicRight", "c"), new JoinIntegrationTest.Input("inputTopicRight", (String)null), new JoinIntegrationTest.Input("inputTopicLeft", (String)null), new JoinIntegrationTest.Input("inputTopicRight", (String)null), new JoinIntegrationTest.Input("inputTopicRight", "d"), new JoinIntegrationTest.Input("inputTopicLeft", "D"));
    private final ValueJoiner<String, String, String> valueJoiner = new ValueJoiner<String, String, String>() {
        public String apply(String value1, String value2) {
            return value1 + "-" + value2;
        }
    };

    public JoinIntegrationTest() {
    }

    @BeforeClass
    public static void setupConfigsAndUtils() {
        PRODUCER_CONFIG.put("bootstrap.servers", CLUSTER.bootstrapServers());
        PRODUCER_CONFIG.put("acks", "all");
        PRODUCER_CONFIG.put("retries", Integer.valueOf(0));
        PRODUCER_CONFIG.put("key.serializer", LongSerializer.class);
        PRODUCER_CONFIG.put("value.serializer", StringSerializer.class);
        RESULT_CONSUMER_CONFIG.put("bootstrap.servers", CLUSTER.bootstrapServers());
        RESULT_CONSUMER_CONFIG.put("group.id", "join-integration-test-result-consumer");
        RESULT_CONSUMER_CONFIG.put("auto.offset.reset", "earliest");
        RESULT_CONSUMER_CONFIG.put("key.deserializer", LongDeserializer.class);
        RESULT_CONSUMER_CONFIG.put("value.deserializer", StringDeserializer.class);
        STREAMS_CONFIG.put("bootstrap.servers", CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put("auto.offset.reset", "earliest");
        STREAMS_CONFIG.put("state.dir", TestUtils.tempDirectory().getPath());
        STREAMS_CONFIG.put("default.key.serde", Serdes.Long().getClass());
        STREAMS_CONFIG.put("default.value.serde", Serdes.String().getClass());
        STREAMS_CONFIG.put("cache.max.bytes.buffering", Integer.valueOf(0));
        STREAMS_CONFIG.put("internal.leave.group.on.close", true);
        STREAMS_CONFIG.put("commit.interval.ms", Integer.valueOf(100));
    }

    @Before
    public void prepareTopology() throws InterruptedException {
        CLUSTER.createTopics(new String[]{"inputTopicLeft", "inputTopicRight", "outputTopic"});
        this.builder = new StreamsBuilder();
        this.leftTable = this.builder.table("inputTopicLeft");
        this.rightTable = this.builder.table("inputTopicRight");
        this.leftStream = this.leftTable.toStream();
        this.rightStream = this.rightTable.toStream();
    }

    @After
    public void cleanup() throws InterruptedException {
        CLUSTER.deleteTopicsAndWait(120000L, new String[]{"inputTopicLeft", "inputTopicRight", "outputTopic"});
    }

    private void checkResult(String outputTopic, List<String> expectedResult) throws InterruptedException {
        if (expectedResult != null) {
            List<String> result = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(RESULT_CONSUMER_CONFIG, outputTopic, expectedResult.size(), 30000L);
            MatcherAssert.assertThat(result, Is.is(expectedResult));
        }

    }

    private void runTest(List<List<String>> expectedResult) throws Exception {
        assert expectedResult.size() == this.input.size();

        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
        KafkaStreams streams = new KafkaStreams(this.builder.build(), STREAMS_CONFIG);

        try {
            streams.start();
            long ts = System.currentTimeMillis();
            Iterator<List<String>> resultIterator = expectedResult.iterator();
            Iterator i$ = this.input.iterator();

            while(i$.hasNext()) {
                JoinIntegrationTest.Input<String> singleInput = (JoinIntegrationTest.Input)i$.next();
                IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(singleInput.topic, Collections.singleton(singleInput.record), PRODUCER_CONFIG, ++ts);
                this.checkResult("outputTopic", (List)resultIterator.next());
            }
        } finally {
            streams.close();
        }

    }

    @Test
    public void testInnerKStreamKStream() throws Exception {
        STREAMS_CONFIG.put("application.id", "join-integration-test-inner-KStream-KStream");
        List<List<String>> expectedResult = Arrays.asList(null, null, null, Collections.singletonList("A-a"), Collections.singletonList("B-a"), Arrays.asList("A-b", "B-b"), null, null, Arrays.asList("C-a", "C-b"), Arrays.asList("A-c", "B-c", "C-c"), null, null, null, Arrays.asList("A-d", "B-d", "C-d"), Arrays.asList("D-a", "D-b", "D-c", "D-d"));
        this.leftStream.join(this.rightStream, this.valueJoiner, JoinWindows.of(10000L)).to("outputTopic");
        this.runTest(expectedResult);
    }

    @Test
    public void testLeftKStreamKStream() throws Exception {
        STREAMS_CONFIG.put("application.id", "join-integration-test-left-KStream-KStream");
        List<List<String>> expectedResult = Arrays.asList(null, null, Collections.singletonList("A-null"), Collections.singletonList("A-a"), Collections.singletonList("B-a"), Arrays.asList("A-b", "B-b"), null, null, Arrays.asList("C-a", "C-b"), Arrays.asList("A-c", "B-c", "C-c"), null, null, null, Arrays.asList("A-d", "B-d", "C-d"), Arrays.asList("D-a", "D-b", "D-c", "D-d"));
        this.leftStream.leftJoin(this.rightStream, this.valueJoiner, JoinWindows.of(10000L)).to("outputTopic");
        this.runTest(expectedResult);
    }

    @Test
    public void testOuterKStreamKStream() throws Exception {
        STREAMS_CONFIG.put("application.id", "join-integration-test-outer-KStream-KStream");
        List<List<String>> expectedResult = Arrays.asList(null, null, Collections.singletonList("A-null"), Collections.singletonList("A-a"), Collections.singletonList("B-a"), Arrays.asList("A-b", "B-b"), null, null, Arrays.asList("C-a", "C-b"), Arrays.asList("A-c", "B-c", "C-c"), null, null, null, Arrays.asList("A-d", "B-d", "C-d"), Arrays.asList("D-a", "D-b", "D-c", "D-d"));
        this.leftStream.outerJoin(this.rightStream, this.valueJoiner, JoinWindows.of(10000L)).to("outputTopic");
        this.runTest(expectedResult);
    }

    @Test
    public void testInnerKStreamKTable() throws Exception {
        STREAMS_CONFIG.put("application.id", "join-integration-test-inner-KStream-KTable");
        List<List<String>> expectedResult = Arrays.asList(null, null, null, null, Collections.singletonList("B-a"), null, null, null, null, null, null, null, null, null, Collections.singletonList("D-d"));
        this.leftStream.join(this.rightTable, this.valueJoiner).to("outputTopic");
        this.runTest(expectedResult);
    }

    @Test
    public void testLeftKStreamKTable() throws Exception {
        STREAMS_CONFIG.put("application.id", "join-integration-test-left-KStream-KTable");
        List<List<String>> expectedResult = Arrays.asList(null, null, Collections.singletonList("A-null"), null, Collections.singletonList("B-a"), null, null, null, Collections.singletonList("C-null"), null, null, null, null, null, Collections.singletonList("D-d"));
        this.leftStream.leftJoin(this.rightTable, this.valueJoiner).to("outputTopic");
        this.runTest(expectedResult);
    }

    @Test
    public void testInnerKTableKTable() throws Exception {
        STREAMS_CONFIG.put("application.id", "join-integration-test-inner-KTable-KTable");
        List<List<String>> expectedResult = Arrays.asList(null, null, null, Collections.singletonList("A-a"), Collections.singletonList("B-a"), Collections.singletonList("B-b"), Collections.singletonList((String)null), null, null, Collections.singletonList("C-c"), Collections.singletonList((String)null), null, null, null, Collections.singletonList("D-d"));
        this.leftTable.join(this.rightTable, this.valueJoiner).to("outputTopic");
        this.runTest(expectedResult);
    }

    @Test
    public void testLeftKTableKTable() throws Exception {
        STREAMS_CONFIG.put("application.id", "join-integration-test-left-KTable-KTable");
        List<List<String>> expectedResult = Arrays.asList(null, null, Collections.singletonList("A-null"), Collections.singletonList("A-a"), Collections.singletonList("B-a"), Collections.singletonList("B-b"), Collections.singletonList((String)null), null, Collections.singletonList("C-null"), Collections.singletonList("C-c"), Collections.singletonList("C-null"), Collections.singletonList((String)null), null, null, Collections.singletonList("D-d"));
        this.leftTable.leftJoin(this.rightTable, this.valueJoiner).to("outputTopic");
        this.runTest(expectedResult);
    }

    @Test
    public void testOuterKTableKTable() throws Exception {
        STREAMS_CONFIG.put("application.id", "join-integration-test-outer-KTable-KTable");
        List<List<String>> expectedResult = Arrays.asList(null, null, Collections.singletonList("A-null"), Collections.singletonList("A-a"), Collections.singletonList("B-a"), Collections.singletonList("B-b"), Collections.singletonList("null-b"), Collections.singletonList((String)null), Collections.singletonList("C-null"), Collections.singletonList("C-c"), Collections.singletonList("C-null"), Collections.singletonList((String)null), null, Collections.singletonList("null-d"), Collections.singletonList("D-d"));
        this.leftTable.outerJoin(this.rightTable, this.valueJoiner).to("outputTopic");
        this.runTest(expectedResult);
    }

    private final class Input<V> {
        String topic;
        KeyValue<Long, String> record;
        private final long anyUniqueKey = 0L;

        Input(String topic,  String var1) {
            this.topic = topic;
            this.record = KeyValue.pair(0L, var1);
        }
    }
}
