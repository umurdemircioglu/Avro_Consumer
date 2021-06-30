package version1;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;


public class AvroConsumer {

    private String schema = "{\"namespace\": \"com.mentor.message\",\n" +
            "    \"type\": \"record\",\n" +
            "    \"name\": \"EventMessage\",\n" +
            "    \"fields\": [\n" +
            "        {\"name\": \"event\", \"type\": \"string\"}\n" +
            "    ]\n" +
            "}";
    Schema avroSchema = new Schema.Parser().parse(schema);


    private boolean control = true;
    public AvroConsumerConfig myConfig;

    public AvroConsumer(AvroConsumerConfig myConfig) {
        this.myConfig = myConfig;
    }

    public KafkaConsumer<String, byte[]> createConsumer(String topic) {
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(myConfig.properties);
        kafkaConsumer.subscribe(Collections.singleton(topic));
        return kafkaConsumer;

    }

    public void start(KafkaConsumer<String, byte[]> kafkaConsumer) {
        System.out.println("Waiting for data...");
        while (control) {
            ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(1000);
            for (ConsumerRecord<String, byte[]> record : records) {
                GenericRecord genericRecord = byteArrayToData(avroSchema, record.value());
                //String oldugu bilindigi icin daha abstract lazim.
                String event = genericRecord.get("event").toString();
                System.out.printf("value = %s \n ", event);
            }
            kafkaConsumer.commitSync();
        }
    }

    private GenericRecord byteArrayToData(Schema schema, byte[] byteData) {
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
}