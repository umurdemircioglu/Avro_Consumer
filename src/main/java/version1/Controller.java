package version1;

import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;


public class Controller {

    public static void main(String[] args) {

        // Config dbden ya da properties vb. bir filedan okunur
        /**
         * <ul>
         *     <li>kafka connection bilgisi, adres topic vb.</li>
         *     <li>hdfste yazılacak yer</li>
         * </ul>
         */
         /*
        String hdfsHost = null;
        String hdfsOutputPath = null;
        int connectionCount = 5;
        */
        // read file
        String kafkaBootstrapServers  = "127.0.0.1:9092";
        String groupID = "customer-consumer-group-v1";
        String autoCommit = "true";
        String offsetReset = "latest";
        String keyDeserializer = StringDeserializer.class.getName();
        String valueDeserializer = ByteArrayDeserializer.class.getName();
        String topic = "customer-avro";
        String schemaPath = "/Users/umurdemircioglu/Desktop/Avro_Consumer/src/main/resources/EventMessage.avsc";


        AvroConsumerConfig config = new AvroConsumerConfig();
        config.setConnectionParams(kafkaBootstrapServers,groupID);
        config.settings(autoCommit,offsetReset,keyDeserializer,valueDeserializer);
        AvroConsumer consumer = new AvroConsumer(config);
        KafkaConsumer<String, byte[]> kafkaConsumer = consumer.createConsumer(topic);
        Schema avroSchema = consumer.parseAvroSchema(schemaPath);
        consumer.start(kafkaConsumer,avroSchema);

    }

}
