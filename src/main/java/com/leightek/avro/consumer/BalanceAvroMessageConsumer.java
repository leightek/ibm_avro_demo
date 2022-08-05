package com.leightek.avro.consumer;

import com.ibm.gbs.schema.Balance;
import com.leightek.avro.util.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class BalanceAvroMessageConsumer {

    public List<Balance> consumeMessage(List<Balance> balanceList) {
        Properties properties = Utils.createConsumerProperties();
        KafkaConsumer<String, Balance> consumer = new KafkaConsumer<String, Balance>(properties);
        String topic = "Balance";

        consumer.subscribe(Collections.singleton(topic));

        System.out.println("Waiting for Balance data...");

        ConsumerRecords<String, Balance> records = consumer.poll(500);
        if (records != null && !records.isEmpty()) {
            if (balanceList == null) {
                balanceList = new ArrayList<>();
            }
            for (ConsumerRecord<String, Balance> record : records) {
                Balance balance = record.value();
                System.out.println(balance);

                balanceList.add(balance);
            }
        }
        consumer.commitSync();

        return balanceList;
    }
}
