package com.leightek.avro.producer;

import com.ibm.gbs.schema.Balance;
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
public class BalanceAvroMessageProducer {

    private static Logger logger = LoggerFactory.getLogger(BalanceAvroMessageProducer.class);

    private Properties producerProperties;

    public BalanceAvroMessageProducer(@Qualifier("kafkaProducerConfigs") Properties producerProperties) {
        this.producerProperties = producerProperties;
    }

    public void produceMessage() {

        KafkaProducer<String, Balance> kafkaProducer = new KafkaProducer<String, Balance>(producerProperties);

        Balance balance = Balance.newBuilder()
                .setBalanceId("j")
                .setAccountId("b")
                .setBalance(20.23f)
                .build();

        String topic = "Balance";
        ProducerRecord<String, Balance> producerRecord = new ProducerRecord<String, Balance>(topic, balance);

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    logger.info("Produce Balance message successfully!");
                    logger.debug(metadata.toString());
                } else {
                    logger.error("Exception encountered during producing Balance message: ", exception);
                }
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
