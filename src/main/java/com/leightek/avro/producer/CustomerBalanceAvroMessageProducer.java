package com.leightek.avro.producer;

import com.ibm.gbs.schema.CustomerBalance;
import com.leightek.avro.util.Utils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class CustomerBalanceAvroMessageProducer {

    private Properties producerConfigs;

    public CustomerBalanceAvroMessageProducer(Properties producerConfigs) {
        this.producerConfigs = producerConfigs;
    }

    public void produceMessage(CustomerBalance customerBalance) {

        Properties properties = Utils.createProducerProperties();
        KafkaProducer<String, CustomerBalance> kafkaProducer = new KafkaProducer<String, CustomerBalance>(properties);
        String topic = "CustomerBalance";

        ProducerRecord<String, CustomerBalance> producerRecord = new ProducerRecord<String, CustomerBalance>(topic,
                customerBalance);

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println("Produce Customer Balance message successfully!");
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
