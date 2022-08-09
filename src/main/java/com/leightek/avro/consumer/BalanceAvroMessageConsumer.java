package com.leightek.avro.consumer;

import com.ibm.gbs.schema.Balance;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Configuration
public class BalanceAvroMessageConsumer {

    private static Logger logger = LoggerFactory.getLogger(BalanceAvroMessageConsumer.class);

    private Properties consumerProperties;

    public BalanceAvroMessageConsumer(@Qualifier("kafkaConsumerConfigs") Properties consumerProperties) {
        this.consumerProperties = consumerProperties;
    }

    public List<Balance> consumeMessage(List<Balance> balanceList) {
        KafkaConsumer<String, Balance> consumer = new KafkaConsumer<String, Balance>(consumerProperties);
        String topic = "Balance";

        consumer.subscribe(Collections.singleton(topic));

        logger.info("Waiting for Balance data...");

        ConsumerRecords<String, Balance> records = consumer.poll(500);
        if (records != null && !records.isEmpty()) {
            if (balanceList == null) {
                balanceList = new ArrayList<>();
            }
            for (ConsumerRecord<String, Balance> record : records) {
                Balance balance = record.value();
                logger.debug(balance.toString());

                balanceList.add(balance);
            }
        }
        consumer.commitSync();

        return balanceList;
    }
}
