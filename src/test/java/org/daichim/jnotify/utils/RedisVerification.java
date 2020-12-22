package org.daichim.jnotify.utils;

import lombok.AllArgsConstructor;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;

public class RedisVerification {

    private Jedis jedis;

    public RedisVerification(String host, int port) {
        this.jedis = new Jedis(host, port);
    }

    public void verify(List<Verification> cmds) {
        for (Verification v : cmds) {
            Object res = v.producer.apply(jedis);
            assertEquals(res, v.result, "Mismatch in redis verification");
        }
    }

    @AllArgsConstructor
    public static class Verification {
        Function<Jedis, Object> producer;
        Object result;

        public static Verification of(Function<Jedis, Object> p, Object r) {
            return new Verification(p, r);
        }
    }
}
