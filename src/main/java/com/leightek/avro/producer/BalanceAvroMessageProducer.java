package com.leightek.avro.producer;

import com.ibm.gbs.schema.Balance;
import com.leightek.avro.util.Utils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class BalanceAvroMessageProducer {

    public void produceMessage() {

        Properties properties = Utils.createProducerProperties();

        KafkaProducer<String, Balance> kafkaProducer = new KafkaProducer<String, Balance>(properties);

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
                    System.out.println("Produce Balance message successfully!");
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
