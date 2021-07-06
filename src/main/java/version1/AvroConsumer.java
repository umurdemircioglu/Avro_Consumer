package version1;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;


public class AvroConsumer {

    private AtomicBoolean control;
    public AvroConsumerConfig config;
    static Logger logger = LogManager.getLogger(AvroConsumer.class);



    public AvroConsumer(AvroConsumerConfig config) {
        this.config = config;
        this.control = new AtomicBoolean(true);
        PropertyConfigurator.configure("src/main/resources/log4j.properties");
    }

    public KafkaConsumer<String, byte[]> createConsumer(String topic) {
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(config.properties);
        kafkaConsumer.subscribe(Collections.singleton(topic));
        return kafkaConsumer;

    }

    public void start(KafkaConsumer<String, byte[]> kafkaConsumer,Schema schema) {
        logger.info("Waiting for data...");
        while (control.get()) {
            ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(1000);
            for (ConsumerRecord<String, byte[]> record : records) {
                GenericRecord genericRecord = byteArrayToData(schema, record.value());
                //+Abstract
                String event = genericRecord.get("event").toString();
                logger.info(event);
            }
            kafkaConsumer.commitSync();
            if(control.get() == false){
                break;
            }
        }
    }

    public void stop(){
        this.control.set(false);
    }

    public Schema parseAvroSchema(String schemaPath){
            Schema finalSchema;
        try{
            finalSchema = new Schema.Parser().parse(new File(schemaPath));
            return finalSchema;
        }catch (IOException e){
            return null;
        }
     }


    public GenericRecord byteArrayToData(Schema schema, byte[] byteData) {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        ByteArrayInputStream byteArrayInputStream = null;
        try {
            byteArrayInputStream = new ByteArrayInputStream(byteData);
            Decoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);

            return reader.read(null, decoder);
        } catch (IOException e) {
            return null;
        } finally {
            try {
                byteArrayInputStream.close();
            } catch (IOException e) {
            }
        }
    }
    //public void <T> getValue(){

    //}
}