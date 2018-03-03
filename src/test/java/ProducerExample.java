
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class ProducerExample {
    public static void main(String[] args){
        try {
            if (args.length != 2) {
                System.out.println("Please provide command line arguments: numEvents schemaRegistryUrl");
                System.exit(-1);
            }
            long events = Long.parseLong(args[0]);
            String url = args[1];

            Properties props = new Properties();
            props.put("bootstrap.servers", "liu:9092");
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            props.put("schema.registry.url", url);

            String schemaString = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                    "\"name\": \"page_visit\"," +
                    "\"fields\": [" +
                    "{\"name\": \"time\", \"type\": \"long\"}," +
                    "{\"name\": \"site\", \"type\": \"string\"}," +
                    "{\"name\": \"ip\", \"type\": \"string\"}" +
                    "]}";
            Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(props);

            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(schemaString);

            Random rnd = new Random();
            for (long nEvents = 0; nEvents < events; nEvents++) {
                long runtime = new Date().getTime();
                String site = "www.example.com";
                String ip = "192.168.2." + rnd.nextInt(255);

                GenericRecord page_visit = new GenericData.Record(schema);
                page_visit.put("time", runtime);
                page_visit.put("site", site);
                page_visit.put("ip", ip);
                System.out.println("sending");
                ProducerRecord<String, GenericRecord> data = new ProducerRecord<String, GenericRecord>(
                        "page_visits", ip, page_visit);
                producer.send(data);
            }

            producer.close();

        }catch (Throwable t) {
            System.out.println(t);
        }
    }
}