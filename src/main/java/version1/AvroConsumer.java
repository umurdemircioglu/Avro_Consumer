package version1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.util.Collections;


public class AvroConsumer {

    private boolean control = true;
    public AvroConsumerConfig myConfig;
    public AvroConsumer(AvroConsumerConfig myConfig){
        this.myConfig = myConfig;
    }

    public KafkaConsumer<String, Customer> createConsumer(String topic){
        KafkaConsumer<String, Customer> kafkaConsumer = new KafkaConsumer<>(myConfig.properties);
        kafkaConsumer.subscribe(Collections.singleton(topic));
        return kafkaConsumer;

    }

    public void start(KafkaConsumer<String, Customer> kafkaConsumer) {
        System.out.println("Waiting for data...");
        while(control){
            ConsumerRecords<String, Customer> records = kafkaConsumer.poll(1000);
            for (ConsumerRecord<String, Customer> record : records){
                Customer customer = record.value();
                System.out.println(customer);
            }
            kafkaConsumer.commitSync();
        }
    }

    public void pause(){
        this.control = false;
    }
    public void resume(){
        this.control = true;
    }

}
