package com.leightek.avro.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class CustomerBalanceAvroMessageProducer implements AvroMessageProducer {

    private static Logger logger = LoggerFactory.getLogger(CustomerBalanceAvroMessageProducer.class);

    private KafkaProducer<String, Object> kafkaProducer;

    public CustomerBalanceAvroMessageProducer(KafkaProducer<String, Object> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void produceMessage() {
        // TODO
    }

    @Override
    public void produceMessage(String topic, Object message) {

        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic,
                message);

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    logger.info("Produce Customer Balance message successfully!");
                    logger.debug(metadata.toString());
                } else {
                    logger.error("Exception encountered during producing Customer Balance message: ", exception);
                }
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
