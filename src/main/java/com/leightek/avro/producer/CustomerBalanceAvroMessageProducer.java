package com.leightek.avro.producer;

import com.ibm.gbs.schema.CustomerBalance;
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
public class CustomerBalanceAvroMessageProducer {

    private static Logger logger = LoggerFactory.getLogger(CustomerBalanceAvroMessageProducer.class);

    private Properties producerProperties;

    public CustomerBalanceAvroMessageProducer(@Qualifier("kafkaProducerConfigs") Properties producerProperties) {
        this.producerProperties = producerProperties;
    }

    public void produceMessage(CustomerBalance customerBalance) {

        KafkaProducer<String, CustomerBalance> kafkaProducer = new KafkaProducer<String, CustomerBalance>(producerProperties);
        String topic = "CustomerBalance";

        ProducerRecord<String, CustomerBalance> producerRecord = new ProducerRecord<String, CustomerBalance>(topic,
                customerBalance);

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
