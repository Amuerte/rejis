package redis.clients.rejis;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Hashing;
import redis.clients.util.Pool;


public class ShardedSlavedJedisPool extends Pool<ShardedJedis> {

    public ShardedSlavedJedisPool(GenericObjectPool.Config poolConfig, List<JedisShardInfo> shards) {
        this(poolConfig, shards, Hashing.MURMUR_HASH);
    }

    public ShardedSlavedJedisPool(GenericObjectPool.Config poolConfig, List<JedisShardInfo> shards, Hashing algo) {
        this(poolConfig, shards, algo, null);
    }

    public ShardedSlavedJedisPool(GenericObjectPool.Config poolConfig, List<JedisShardInfo> shards, Pattern keyTagPattern) {
        this(poolConfig, shards, Hashing.MURMUR_HASH, keyTagPattern);
    }

    public ShardedSlavedJedisPool(GenericObjectPool.Config poolConfig, List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern) {
        super(poolConfig, new SlavedShardedJedisFactory(shards, algo, keyTagPattern));
    }

    private static class SlavedShardedJedisFactory extends BasePoolableObjectFactory {
        private List<JedisShardInfo> shards;
        private Hashing algo;
        private Pattern keyTagPattern;

        public SlavedShardedJedisFactory(List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern) {
            this.shards = shards;
            this.algo = algo;
            this.keyTagPattern = keyTagPattern;
        }

        // We register the jedis as slaves.
        public Object makeObject() throws Exception {
            ShardedJedis shardedJedis = new ShardedJedis(this.shards, this.algo, this.keyTagPattern);
            JedisShardInfo shardInfo = this.shards.get(0);
            for (Jedis jedis : shardedJedis.getAllShards()) {
                jedis.slaveof(shardInfo.getHost(), shardInfo.getPort());
            }
            return shardedJedis;
        }

        public void destroyObject(Object obj) throws Exception {
            if ((obj != null) && (obj instanceof ShardedJedis)) {
                ShardedJedis shardedJedis = (ShardedJedis) obj;
                for (Jedis jedis : shardedJedis.getAllShards())
                    try {
                        try {
                            jedis.quit();
                        } catch (Exception e) {
                        }
                        jedis.disconnect();
                    } catch (Exception e) {
                    }
            }
        }

        public boolean validateObject(Object obj) {
            try {
                ShardedJedis jedis = (ShardedJedis) obj;
                for (Jedis shard : jedis.getAllShards()) {
                    if (!(shard.ping().equals("PONG"))) {
                        return false;
                    }
                }
                return true;
            } catch (Exception ex) {
            }
            return false;
        }
    }
}
