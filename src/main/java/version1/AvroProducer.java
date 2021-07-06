package version1;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class AvroProducer {
    public static void main(String[] args) {
        String topic = "customer-avro";
        String schemaPath = "/Users/umurdemircioglu/Desktop/Avro_Consumer/src/main/resources/EventMessage.avsc";


        Schema schema = createAvroSchema(schemaPath);

        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", ByteArraySerializer.class.getName());

        System.out.println("" + properties);

        Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties);


        String eventName = "argela";
        GenericRecord record = new GenericData.Record(schema);
        record.put("event", eventName);



        byte[] last = null;

        // Read as GenericRecord
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder enc = EncoderFactory.get().binaryEncoder(out, null);
        try {
            writer.write(record, enc);
            enc.flush();
            byte[] byteData = out.toByteArray();
            last = byteData;
        } catch (IOException ioException) {
            ioException.printStackTrace();

        }finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<String, byte[]>(topic,0,Integer.toString(0),last);

        producer.send(producerRecord);
        producer.flush();
        producer.close();



    }

    public static Schema createAvroSchema(String schemaPath){
        Schema finalSchema;
        try{
            finalSchema = new Schema.Parser().parse(new File(schemaPath));
            return finalSchema;
        }catch (IOException e){
            return null;
        }
        //final DataFileReader<GenericRecord> genericRecords = new DataFileReader<>(avroFile, genericDatumReader);
    }
}
