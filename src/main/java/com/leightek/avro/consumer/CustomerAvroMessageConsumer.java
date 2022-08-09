package com.leightek.avro.consumer;

import com.ibm.gbs.schema.Customer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Component
public class CustomerAvroMessageConsumer {

    private static Logger logger = LoggerFactory.getLogger(CustomerAvroMessageConsumer.class);

    private Properties consumerProperties;

    public CustomerAvroMessageConsumer(@Qualifier("kafkaConsumerConfigs") Properties consumerProperties) {
        this.consumerProperties = consumerProperties;
    }

    public List<Customer> consumeMessage(List<Customer> customers) {
        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<String, Customer>(consumerProperties);
        String topic = "Customer";

        consumer.subscribe(Collections.singleton(topic));

        logger.info("Waiting for Customer data...");

        ConsumerRecords<String, Customer> records = consumer.poll(500);
        if (records != null && !records.isEmpty()) {
            for (ConsumerRecord<String, Customer> record : records) {
                Customer customer = record.value();
                logger.debug(customer.toString());

                customers.add(customer);
            }
        }
        consumer.commitSync();

        return customers;
    }
}
