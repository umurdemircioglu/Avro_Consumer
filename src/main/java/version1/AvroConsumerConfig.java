package version1;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AvroConsumerConfig {
    public Properties properties;

    public AvroConsumerConfig(){
        this.properties = new Properties();
    }

    public void setParameters(String configPath){
        try (InputStream input = new FileInputStream(configPath)) {
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    }


