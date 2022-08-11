package com.leightek.avro.consumer;

import com.ibm.gbs.schema.Customer;
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
public class CustomerAvroMessageConsumer implements AvroMessageConsumer {

    private static Logger logger = LoggerFactory.getLogger(CustomerAvroMessageConsumer.class);

    private KafkaConsumer<String, Object> kafkaConsumer;

    public CustomerAvroMessageConsumer(KafkaConsumer<String, Object> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public List<Object> consumeMessage() {

        String topic = "Customer";
        List<Object> customers = new ArrayList<>();

        kafkaConsumer.subscribe(Collections.singleton(topic));

        logger.info("Waiting for Customer data...");

        ConsumerRecords<String, Object> records = kafkaConsumer.poll(500);
        if (records != null && !records.isEmpty()) {
            for (ConsumerRecord<String, Object> record : records) {
                Customer customer = (Customer) record.value();
                logger.debug(customer.toString());

                customers.add(customer);
            }
        }
        kafkaConsumer.commitSync();

        return customers;
    }
}
