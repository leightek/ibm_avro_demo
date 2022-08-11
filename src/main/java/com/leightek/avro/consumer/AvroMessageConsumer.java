package com.leightek.avro.consumer;

import java.util.List;

public interface AvroMessageConsumer {
    List<Object> consumeMessage();
}
