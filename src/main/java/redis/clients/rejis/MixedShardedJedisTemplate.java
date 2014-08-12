package redis.clients.rejis;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

public class MixedShardedJedisTemplate extends AbstractJedisTemplate<ShardedJedis, Jedis> {

    public MixedShardedJedisTemplate(JedisPoolConfig jedisPoolConfig, String masterIP, int masterPort, String password, List<JedisShardInfo> shards) {
        this.setWritePool(new JedisPool(jedisPoolConfig, masterIP, masterPort));
        this.setReadPool(new ShardedJedisPool(jedisPoolConfig, shards));
    }

    public Long del(final String... keys) {
        return doPut(new JedisCallback<Jedis, Long>() {
            public Long doJedisAction(Jedis jedis) {
                return jedis.del(keys);
            }
        });
    }

    @Override
    protected Jedis getJedisFromWritePool(Pool<Jedis> writePool) {
        Jedis jedis = super.getJedisFromWritePool(writePool);
        validClientConnection(writePool, jedis);

        return jedis;
    }

    protected void validClientConnection(Pool<Jedis> pool, Jedis jedis) {
        if (null != jedis) { // utile ?
            if (!jedis.isConnected()) {
                try {
                    jedis.connect();
                } catch (JedisConnectionException e) {
                    returnBrokenClientToPool(jedis, pool);
                    throw e;
                }
            }
        }
    }
}
