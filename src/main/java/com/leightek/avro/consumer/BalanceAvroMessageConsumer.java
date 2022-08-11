package com.leightek.avro.consumer;

import com.ibm.gbs.schema.Balance;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class BalanceAvroMessageConsumer implements AvroMessageConsumer {

    private static Logger logger = LoggerFactory.getLogger(BalanceAvroMessageConsumer.class);

    private KafkaConsumer<String, Object> kafkaConsumer;

    public BalanceAvroMessageConsumer(KafkaConsumer<String, Object> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public List<Object> consumeMessage() {

        String topic = "Balance";
        List<Object> balanceList = new ArrayList<>();

        kafkaConsumer.subscribe(Collections.singleton(topic));

        logger.info("Waiting for Balance data...");

        ConsumerRecords<String, Object> records = kafkaConsumer.poll(500);
        if (records != null && !records.isEmpty()) {
            if (balanceList == null) {
                balanceList = new ArrayList<>();
            }
            for (ConsumerRecord<String, Object> record : records) {
                Balance balance = (Balance) record.value();
                logger.debug(balance.toString());

                balanceList.add(balance);
            }
        }
        kafkaConsumer.commitSync();

        return balanceList;
    }
}
