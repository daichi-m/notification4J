package com.walmart.analytics.platform.notifier.impl;
import com.walmart.analytics.platform.notifier.model.NotificationConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Provider;

public class JedisProducer implements Provider<JedisPool> {

    @Inject
    private NotificationConfiguration configuration;

    private JedisPool jedisPool;

    @PostConstruct
    public void constructPool() {

        GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxIdle(configuration.getIdleRedisConnections());
        poolConfig.setMaxTotal(configuration.getMaxRedisConnections());

        String host = configuration.getRedisHost();
        int port = configuration.getRedisPort();
        String password = configuration.getRedisPassword();
        int timeout = configuration.getRedisConnectionTimeout();
        int database = configuration.getRedisDatabase();
        boolean useSsl = configuration.isUseSsl();
        this.jedisPool =  StringUtils.isNotBlank(password)
            ? new JedisPool(poolConfig, host, port, timeout, password, database, useSsl)
            : new JedisPool(poolConfig, host, port, timeout, null, database, useSsl);
    }

    public JedisPool get() {
        return this.jedisPool;
    }
}
