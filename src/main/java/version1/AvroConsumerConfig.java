package version1;

import java.util.Properties;

public class AvroConsumerConfig {
    public Properties properties;

    public AvroConsumerConfig(Properties properties){
        this.properties = properties;
    }

    public void connect(String kafkaBootstrapServers, String groupID){
        properties.setProperty("bootstrap.servers",kafkaBootstrapServers);
        properties.put("group.id", groupID);
    }

    public void settings(String autoCommit, String offsetReset, String keyDeserializer, String valueDeserializer, String schemaConnect, String avroReader){
        properties.put("auto.commit.enable", autoCommit);
        properties.put("auto.offset.reset", offsetReset);
        properties.setProperty("key.deserializer", keyDeserializer);
        properties.setProperty("value.deserializer", valueDeserializer);
        properties.setProperty("schema.registry.url", schemaConnect);
        properties.setProperty("specific.avro.reader", avroReader);
    }

}
