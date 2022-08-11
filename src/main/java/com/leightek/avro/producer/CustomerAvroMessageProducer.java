package com.leightek.avro.producer;

import com.ibm.gbs.schema.Customer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class CustomerAvroMessageProducer implements AvroMessageProducer {

    private static Logger logger = LoggerFactory.getLogger(CustomerAvroMessageProducer.class);

    private KafkaProducer<String, Object> kafkaProducer;

    public CustomerAvroMessageProducer(KafkaProducer<String, Object> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void produceMessage() {

        String topic = "Customer";

        Customer customer = Customer.newBuilder()
                .setCustomerId("a")
                .setName("mehryar")
                .setPhoneNumber("888-888-8888")
                .setAccountId("b")
                .build();

        produceMessage(topic, customer);

    }

    @Override
    public void produceMessage(String topic, Object message) {
        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, message);

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
