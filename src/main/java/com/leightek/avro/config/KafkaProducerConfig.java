package com.leightek.avro.config;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.Properties;

@Configuration
public class KafkaProducerConfig {

    @Resource(name = "kafkaBaseConfig")
    Properties baseProperties;

    @Value(value = "${acks}")
    String acks;

    @Value(value = "${retries}")
    String retries;

    @Value(value = "${schema.registry.url}")
    String schemaRegistryUrl;

    private Properties getKafkaProducerConfigs() {
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", (String) baseProperties.get("bootstrap.servers"));
        properties.setProperty("schema.registry.url", (String) baseProperties.get("schema.registry.url"));
        properties.setProperty("acks", acks);
        properties.setProperty("retries", retries);

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());

        return properties;
    }

    @Bean
    public KafkaProducer<String, Object> kafkaProducer() {
        return new KafkaProducer<>(getKafkaProducerConfigs());
    }

}
