package com.leightek.avro.producer;

import com.ibm.gbs.schema.Customer;
import com.leightek.avro.util.Utils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class CustomerAvroMessageProducer {

    private Properties producerConfigs;

    public CustomerAvroMessageProducer(Properties producerConfigs) {
        this.producerConfigs = producerConfigs;
    }

    public void produceMessage() {

        Properties properties = Utils.createProducerProperties();
        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<String, Customer>(properties);
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
                    System.out.println("Produce Customer message successfully!");
                    System.out.println(metadata.toString());
                } else {
                    exception.printStackTrace();
                }
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
