package com.leightek.avro.producer;

import com.ibm.gbs.schema.Balance;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class BalanceAvroMessageProducer implements AvroMessageProducer {

    private static Logger logger = LoggerFactory.getLogger(BalanceAvroMessageProducer.class);

    private KafkaProducer<String, Object> kafkaProducer;

    public BalanceAvroMessageProducer(KafkaProducer<String, Object> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void produceMessage() {

        Balance balance = Balance.newBuilder()
                .setBalanceId("j")
                .setAccountId("b")
                .setBalance(20.23f)
                .build();

        String topic = "Balance";

        produceMessage(topic, balance);
    }

    @Override
    public void produceMessage(String topic, Object message) {
        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, message);

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
