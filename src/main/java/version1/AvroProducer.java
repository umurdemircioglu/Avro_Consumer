package version1;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AvroProducer {
    public static void main(String[] args) {
        String topic = "customer-avro";

        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        System.out.println("" + properties);

        Producer<String, Customer> producer = new KafkaProducer<String, Customer>(properties);



        Customer customer = Customer.newBuilder()
                .setAge(24)
                .setAutomatedEmail(false)
                .setFirstName("Emre")
                .setLastName("Tan")
                .setHeight(150f)
                .setWeight(90f)
                .build();

        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<String, Customer>(topic, customer);

        System.out.println(customer);
        producer.send(producerRecord);

        producer.flush();
        producer.close();



    }
}
