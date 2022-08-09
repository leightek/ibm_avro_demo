package com.leightek.avro.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.Properties;

@Configuration
public class KafkaConsumerConfig {

    @Resource(name = "kafkaBaseConfig")
    Properties baseProperties;

    @Value(value = "${group.id}")
    String groupId;

    @Value(value = "${enable.auto.commit}")
    String autoCommit;

    @Value(value = "${auto.offset.reset}")
    String autoOffsetReset;

    @Value(value = "specific.avro.reader")
    String isSpecificAvroReader;

    @Bean("kafkaConsumerConfigs")
    public Properties kafkaConsumerConfigs() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", (String) baseProperties.get("bootstrap.servers"));
        properties.setProperty("schema.registry.url", (String) baseProperties.get("schema.registry.url"));
        properties.setProperty("group.id", groupId);
        properties.setProperty("enable.auto.commit", autoCommit);
        properties.setProperty("auto.offset.reset", autoOffsetReset);

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("specific.avro.reader", isSpecificAvroReader);

        return properties;
    }
}
