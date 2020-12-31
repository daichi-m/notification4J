package io.github.daichim.notification4J.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import io.github.daichim.notification4J.model.NotificationConfiguration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.util.Pool;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashSet;
import java.util.Set;

@Slf4j
@Named
public class JedisFactory {

    @Inject
    private NotificationConfiguration configuration;

    private Pool<Jedis> jedisPool;

    @PostConstruct
    public void constructPool() {

        GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxIdle(configuration.getIdleRedisConnections());
        poolConfig.setMaxTotal(configuration.getMaxRedisConnections());

        String host = configuration.getRedisHost();
        int port = configuration.getRedisPort();
        Set<String> sentinels = new HashSet<>(configuration.getRedisSentinels());
        String master = configuration.getSentinelMaster();
        String passwd = configuration.getRedisPassword();
        int to = configuration.getRedisConnectionTimeout();
        int db = configuration.getRedisDatabase();
        boolean ssl = configuration.isUseSsl();

        boolean sentinelMode = StringUtils.isNotBlank(master) &&
            CollectionUtils.isNotEmpty(sentinels);

        if (sentinelMode) {
            this.jedisPool = StringUtils.isNotBlank(passwd)
                ? new JedisSentinelPool(master, sentinels, poolConfig, to, passwd, db)
                : new JedisSentinelPool(master, sentinels, poolConfig, to, null, db);
        } else {
            this.jedisPool = StringUtils.isNotBlank(passwd)
                ? new JedisPool(poolConfig, host, port, to, passwd, db, ssl)
                : new JedisPool(poolConfig, host, port, to, null, db, ssl);
        }
    }

    public Jedis get() {
        return jedisPool.getResource();
    }
}
