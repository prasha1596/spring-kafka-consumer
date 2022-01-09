package com.prachi.kafka.springbootkafkaconsumer.config;

import com.prachi.kafka.springbootkafkaconsumer.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "grp_STRING");
        return new DefaultKafkaConsumerFactory<>(config);

        //inject consumer factory to kafka listener consumer factory
    }

    @Bean(name = "default")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, User> userConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "grp_JSON");
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new JsonDeserializer<>(User.class));
    }

    @Bean(name = "jsonCustomized")
    public ConcurrentKafkaListenerContainerFactory<String, User> userKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, User> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(userConsumerFactory());
        factory.setRetryTemplate(retryTemplate());
        factory.setErrorHandler(new ErrorHandler() {
            @Override
            public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer container) {
                String s = thrownException.getMessage().split("Error deserializing key/value for partition ")[1].split(". If needed, please seek past the record to continue consumption.")[0];
                String topics = s.split("-")[0];
                int offset = Integer.valueOf(s.split("offset ")[1]);
                int partition = Integer.valueOf(s.split("-")[1].split(" at")[0]);

                TopicPartition topicPartition = new TopicPartition(topics, partition);
                log.info("Skipping " + topicPartition + "-" + partition + " offset " + offset);
                consumer.seek(topicPartition, offset + 1);
                System.out.println("Error while deserialization");
            }

            @Override
            public void handle(Exception e, ConsumerRecord<?, ?> consumerRecord) {

            }

//            @Override
//            public void handle(Exception e, ConsumerRecord<?, ?> consumerRecord, Consumer<?,?> consumer) {
//                String s = e.getMessage().split("Error deserializing key/value for partition ")[1].split(". If needed, please seek past the record to continue consumption.")[0];
//                String topics = s.split("-")[0];
//                int offset = Integer.valueOf(s.split("offset ")[1]);
//                int partition = Integer.valueOf(s.split("-")[1].split(" at")[0]);
//
//                TopicPartition topicPartition = new TopicPartition(topics, partition);
//                //log.info("Skipping " + topic + "-" + partition + " offset " + offset);
//                consumer.seek(topicPartition, offset + 1);
//                System.out.println("OKKKKK");
//
//
//            }
        });

//        factory.setErrorHandler(((exception, data) -> {
//            /*
//             * here you can do you custom handling, I am just logging it same as default
//             * Error handler does If you just want to log. you need not configure the error
//             * handler here. The default handler does it for you. Generally, you will
//             * persist the failed records to DB for tracking the failed records.
//             */
//            log.error("Error in process with Exception {} and the record is {}", exception, data);
//            System.out.println("Error in process with Exception" + exception.getMessage() + data);
//        }));

        return factory;
    }

    private RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        /*
         * here retry policy is used to set the number of attempts to retry and what
         * exceptions you wanted to try and what you don't want to retry.
         */
        retryTemplate.setRetryPolicy(retryPolicy());

        return retryTemplate;
    }

    private SimpleRetryPolicy retryPolicy() {
        Map<Class<? extends Throwable>, Boolean> exceptionMap = new HashMap<>();

        // the boolean value in the map determines whether exception should be retried
        exceptionMap.put(IllegalArgumentException.class, false);
        exceptionMap.put(TimeoutException.class, true);
        exceptionMap.put(ListenerExecutionFailedException.class, true);

        return new SimpleRetryPolicy(3, exceptionMap, true);
    }
}
