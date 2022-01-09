package com.prachi.kafka.springbootkafkaconsumer.listener;

import com.prachi.kafka.springbootkafkaconsumer.model.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @KafkaListener(topics = "topic_string_data", groupId = "grp_1", containerFactory = "default")
    public void listen(String message) {
        System.out.println("Consumed message " + message);
    }

    @KafkaListener(topics = "topic_json_data", containerFactory = "jsonCustomized", groupId = "grp_JSON")
    public void consumeJson(User user) {
        System.out.println("Consumed json " + user);
    }
}
