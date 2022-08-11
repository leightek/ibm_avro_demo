package com.leightek.avro.config;

import com.leightek.avro.consumer.BalanceAvroMessageConsumer;
import com.leightek.avro.consumer.CustomerAvroMessageConsumer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

    private Properties getKafkaConsumerConfigs() {
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

    @Bean
    public KafkaConsumer<String, Object> kafkaConsumer() {
        return new KafkaConsumer<>(getKafkaConsumerConfigs());
    }

    @Bean
    public CustomerAvroMessageConsumer customerAvroMessageConsumer() {
        return new CustomerAvroMessageConsumer(kafkaConsumer());
    }

    @Bean
    public BalanceAvroMessageConsumer balanceAvroMessageConsumer() {
        return new BalanceAvroMessageConsumer(kafkaConsumer());
    }


}
