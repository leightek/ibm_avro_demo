package com.leightek.avro.producer;

import com.ibm.gbs.schema.Customer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class CustomerAvroMessageProducer {

    private static Logger logger = LoggerFactory.getLogger(CustomerAvroMessageProducer.class);

    private Properties producerProperties;

    public CustomerAvroMessageProducer(@Qualifier("kafkaProducerConfigs") Properties producerProperties) {
        this.producerProperties = producerProperties;
    }

    public void produceMessage() {

        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<String, Customer>(producerProperties);
        String topic = "Customer";

        Customer customer = Customer.newBuilder()
                .setCustomerId("a")
                .setName("mehryar")
                .setPhoneNumber("888-888-8888")
                .setAccountId("b")
                .build();

        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<String, Customer>(topic, customer);

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    logger.info("Produce Customer message successfully!");
                    logger.debug(metadata.toString());
                } else {
                    logger.error("Exception encountered during producing Customer message: ", exception);
                }
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
