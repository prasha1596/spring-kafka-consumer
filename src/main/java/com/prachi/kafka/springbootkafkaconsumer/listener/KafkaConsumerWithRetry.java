package com.prachi.kafka.springbootkafkaconsumer.listener;

import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.Date;

import static org.springframework.kafka.retrytopic.TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE;

@Component
public class KafkaConsumerWithRetry {

    int i = 1;

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 900000, multiplier = 4.0, maxDelay = 21600000), //21600000 ms - 6 hours or  5400000 - 90 min
            autoCreateTopics = "false", //on local machine - set auto.create.topics.enable = false in server.properties file
            topicSuffixingStrategy = SUFFIX_WITH_INDEX_VALUE) // get topics created first - topic_string_data-retry-0, topic_string_data-retry-1, topic_string_data-retry-2
    @KafkaListener(topics = "topic_string_data", containerFactory = "default")
    public void consume(@Payload String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        System.out.println("retry method invoked -> " + i++ + " times from topic: " + topic + "current time: " + new Date());
        throw new RuntimeException();
    }

    @DltHandler
    public void listenDlt(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(KafkaHeaders.OFFSET) long offset) {
        System.out.println("DLT Received: " + in + " from " + topic + " offset " + offset + " -> " + i++ + " times" + "\n current time dlt: " + new Date());
        //dump event to dlt queue
    }
}
