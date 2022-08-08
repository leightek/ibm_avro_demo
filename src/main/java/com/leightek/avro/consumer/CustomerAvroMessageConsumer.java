package com.leightek.avro.consumer;

import com.ibm.gbs.schema.Customer;
import com.leightek.avro.util.Utils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Component
public class CustomerAvroMessageConsumer {

    private ConsumerConfig consumerConfigs;

    public CustomerAvroMessageConsumer(ConsumerConfig consumerConfigs) {
        this.consumerConfigs = consumerConfigs;
    }

    public List<Customer> consumeMessage(List<Customer> customers) {
        Properties properties = Utils.createConsumerProperties();
        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<String, Customer>(properties);
        String topic = "Customer";

        consumer.subscribe(Collections.singleton(topic));

        System.out.println("Waiting for Customer data...");

        ConsumerRecords<String, Customer> records = consumer.poll(500);
        if (records != null && !records.isEmpty()) {
            for (ConsumerRecord<String, Customer> record : records) {
                Customer customer = record.value();
                System.out.println(customer);

                customers.add(customer);
            }
        }
        consumer.commitSync();

        return customers;
    }
}
