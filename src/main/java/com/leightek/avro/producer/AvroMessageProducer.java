package com.leightek.avro.producer;

public interface AvroMessageProducer {
    void produceMessage();

    void produceMessage(String topic, Object message);
}
