package com.prachi.kafka.springbootkafkaconsumer.listener;


import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumerWithRetry {


    @RetryableTopic(
            attempts = "5",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            autoCreateTopics = "false"
    )
    @KafkaListener(id="orig", topics = "topic_string_data", containerFactory = "default", groupId = "diff")
    public void consume(@Payload String message) {
        System.out.println("retry method invoked");
    }

    @DltHandler
    public void listenDlt(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(KafkaHeaders.OFFSET) long offset) {

        System.out.println("DLT Received: " + in + " from " + topic + " offset " + offset);
        //dump event to dlt queue
    }
}
