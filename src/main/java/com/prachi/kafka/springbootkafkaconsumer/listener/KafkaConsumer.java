package com.prachi.kafka.springbootkafkaconsumer.listener;

import com.prachi.kafka.springbootkafkaconsumer.model.User;
import com.prachi.kafka.springbootkafkaconsumer.util.KafkaHeadersUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @KafkaListener(topics = "topic_string", containerFactory = "default")
    public void listen(@Payload ConsumerRecord<String, String> message, @Header(KafkaHeaders.RECEIVED_TOPIC) String tname) {
        System.out.println(tname);
        System.out.println("Consumed message " + message);
        Headers headers = message.headers();

        if (KafkaHeadersUtil.getHeaderValue(message.headers(), "retryCount") == null)
            KafkaHeadersUtil.setHeaderValue(headers, "retryCount", "1");
        else {
            String retryCount = KafkaHeadersUtil.getHeaderValue(message.headers(), "retryCount");
            KafkaHeadersUtil.setHeaderValue(headers, "retryCount", String.valueOf(Integer.valueOf(retryCount) + 1));
        }
        //process - business logic
        System.out.println("message headers are: "+message.headers());
    }


   // @KafkaListener(topics = "orig1-retry", containerFactory = "retryCOntainer")
    //addtnal handling to check for retry count
    //process - business logic
    //log events after retry exhaust

    @KafkaListener(topics = "topic_json_data", containerFactory = "jsonCustomized", groupId = "grp_JSON")
    public void consumeJson(User user) {
        System.out.println("Consumed json " + user);
    }
}
