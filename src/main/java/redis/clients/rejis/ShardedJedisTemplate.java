package redis.clients.rejis;

import java.util.List;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.Pool;

public class ShardedJedisTemplate extends AbstractJedisTemplate<ShardedJedis, ShardedJedis> {

    public ShardedJedisTemplate(JedisPoolConfig jedisPoolConfig, List<JedisShardInfo> shards) {
        Pool<ShardedJedis> shardedPool = new ShardedJedisPool(jedisPoolConfig, shards);
        this.setWritePool(shardedPool);
        this.setReadPool(shardedPool);
    }

    public Long del(final String... keys) {
        return doPut(new JedisCallback<ShardedJedis, Long>() {
            public Long doJedisAction(ShardedJedis jedis) {
                return jedis.del(keys[0]);
            }
        });
    }
}

