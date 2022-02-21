package com.prachi.kafka.springbootkafkaconsumer.util;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class KafkaHeadersUtil {

    public static String getHeaderValue(Headers headers, String key) {
        if (Objects.nonNull(headers)) {
            for (Header header : headers) {
                if (key.equals(header.key()) && Objects.nonNull(header.value())) {
                    return new String(header.value());
                }
            }
        }
        return null;
    }

    public static void setHeaderValue(Headers headers, String key, String value) {
        boolean flag = false;
        for (Header header : headers) {
            if (key.equals(header.key())) {
                flag = true;
            }
        }
        if (flag) {
            headers.remove(key);
        }
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    }
}