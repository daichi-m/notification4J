package org.daichim.jnotify.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.daichim.jnotify.model.Notification;
import org.daichim.jnotify.utils.TestUtils;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.annotations.BeforeClass;

import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class NotificationSerDeTest {

    @Spy
    ObjectMapper objectMapper;
    @InjectMocks
    NotificationSerDe serde;

    Notification testNotfn;
    String notfnJSON;


    @BeforeClass
    @SneakyThrows
    public void initSerde() {
        objectMapper = new ObjectMapper();
        MockitoAnnotations.initMocks(this);
        serde.initialize();
        testNotfn = TestUtils.randomNotification();
        testNotfn.setId("45");
        notfnJSON = objectMapper.writeValueAsString(testNotfn);
    }

    @Test
    public void serializeSuccess() {
        Optional<String> json = serde.safeSerialize(testNotfn);
        assertTrue(json.isPresent());
        assertEquals(json.get(), notfnJSON);
    }

    @Test
    public void serializeFailure() {
        Optional<String> json = serde.safeSerialize(null);
        assertFalse(json.isPresent());
//        assertEquals(json.get(), notfnJSON);
    }

    @Test
    public void deserializeSuccess() {
        Optional<Notification> notfn = serde.safeDeserialize(notfnJSON);
        assertTrue(notfn.isPresent());
        assertEquals(notfn.get(), testNotfn);
    }

    @Test
    public void deserializeFailure_invalidJSON() {
        Optional<Notification> json = serde.safeDeserialize("Hello World");
        assertFalse(json.isPresent());
    }

    @Test
    public void deserializeFailure_nullJSON() {
        Optional<Notification> json = serde.safeDeserialize(null);
        assertFalse(json.isPresent());
    }
}
