package com.leightek.avro.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaConfig {

    @Value(value = "${kafka.bootstrap.servers:}")
    String bootstrapServers;

    @Value(value = "${avro.schema.registry.url:}")
    String avroSchemaRegistryUrl;

    @Bean(name = "kafkaBaseConfig")
    public Properties getKafkaConfiguration() {
        Properties properties = new Properties();

        if (!bootstrapServers.isEmpty()) {
            properties.setProperty("bootstrap.servers", bootstrapServers);
        }

        if (!avroSchemaRegistryUrl.isEmpty()) {
            properties.setProperty("schema.registry.url", avroSchemaRegistryUrl);
        }

        return properties;
    }
}
