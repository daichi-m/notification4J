package io.github.daichim.notification4J.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.github.daichim.notification4J.model.Notification;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.*;
import java.util.Objects;
import java.util.Optional;

@Slf4j
@Named
public class NotificationSerDe {

    private ThreadLocal<ObjectWriter> serializer;
    private ThreadLocal<ObjectReader> deserializer;

    @Inject
    private ObjectMapper objectMapper;

    @PostConstruct
    public void initialize() {
        serializer =
            ThreadLocal.withInitial(() -> this.objectMapper.writerFor(Notification.class));
        deserializer =
            ThreadLocal.withInitial(() -> this.objectMapper.readerFor(Notification.class));
    }

    /**
     * Deserialize a JSON to {@link Notification} without throwing exception
     *
     * @param json The JSON to deserialize
     *
     * @return The {@link Notification} object corresponding to the JSON, or {@link Optional#empty}
     *     in case of errors.
     */
    public Optional<Notification> safeDeserialize(String json) {
        try {
            if (StringUtils.isEmpty(json)) {
                return Optional.empty();
            }
            return Optional.of(deserializer.get().readValue(json));
        } catch (IOException ex) {
            log.warn("Deserialization error: {}", json, ex);
            return Optional.empty();
        }
    }

    /**
     * Serialize a {@link Notification} to JSON String without throwing exception
     *
     * @param notification The {@link Notification} object to convert to JSON
     *
     * @return The JSON String wrapped or {@link Optional#empty()} in case of errors.
     */
    public Optional<String> safeSerialize(Notification notification) {
        try {
            if (Objects.isNull(notification)) {
                return Optional.empty();
            }
            return Optional.of(serializer.get().writeValueAsString(notification));
        } catch (IOException ex) {
            log.warn("Serialization error: {}", notification, ex);
            return Optional.empty();
        }
    }

}
