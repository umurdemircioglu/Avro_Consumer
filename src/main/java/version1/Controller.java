package version1;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

        String kafkaBootstrapServers  = "127.0.0.1:9092";
        String groupID = "customer-consumer-group-v1";
        String autoCommit = "false";
        String offsetReset = "latest";
        String keyDeserializer = StringDeserializer.class.getName();
        String valueDeserializer = KafkaAvroDeserializer.class.getName();
        String schemaConnect = "http://127.0.0.1:8081";
        String avroReader = "true";
        String topic = "customer-avro";

        AvroConsumerConfig myConfig = new AvroConsumerConfig();
        myConfig.connect(kafkaBootstrapServers,groupID);
        myConfig.settings(autoCommit,offsetReset,keyDeserializer,valueDeserializer,schemaConnect,avroReader);
        AvroConsumer consumer = new AvroConsumer(myConfig);
        KafkaConsumer<String, Customer> kafkaConsumer = consumer.createConsumer(topic);
        consumer.start(kafkaConsumer);

    }

}
