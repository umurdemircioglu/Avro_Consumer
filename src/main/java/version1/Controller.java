package version1;

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

        String kafkaBootstrapServers  = "127.0.0.1:9092";
        String groupID = "customer-consumer-group-v1";
        String autoCommit = "true";
        String offsetReset = "latest";
        String keyDeserializer = StringDeserializer.class.getName();
        String valueDeserializer = ByteArrayDeserializer.class.getName();
        String topic = "customer-avro";

        AvroConsumerConfig myConfig = new AvroConsumerConfig();
        myConfig.connect(kafkaBootstrapServers,groupID);
        myConfig.settings(autoCommit,offsetReset,keyDeserializer,valueDeserializer);
        AvroConsumer consumer = new AvroConsumer(myConfig);
        KafkaConsumer<String, byte[]> kafkaConsumer = consumer.createConsumer(topic);
        consumer.start(kafkaConsumer);

    }

}
