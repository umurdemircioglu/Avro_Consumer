package version1;

import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Controller {

    public static void main(String[] args) {
        String topic = "customer-avro";
        String schemaPath = "/Users/umurdemircioglu/Desktop/Avro_Consumer/src/main/resources/EventMessage.avsc";
        String configPath ="src/main/resources/config.properties";


        AvroConsumerConfig config = new AvroConsumerConfig();
        config.setParameters(configPath);
        AvroConsumer consumer = new AvroConsumer(config);
        KafkaConsumer<String, byte[]> kafkaConsumer = consumer.createConsumer(topic);
        Schema avroSchema = consumer.parseAvroSchema(schemaPath);
        consumer.start(kafkaConsumer,avroSchema);

    }

}
