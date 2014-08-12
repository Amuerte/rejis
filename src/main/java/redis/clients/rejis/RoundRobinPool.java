package redis.clients.rejis;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

/**
 * https://gist.github.com/ib84/1084272
 */
public class RoundRobinPool extends Pool<Jedis> {
    private GenericObjectPool jedisClientPool;
    private RoundRobinFactory factory;

    public RoundRobinPool(final GenericObjectPool.Config poolConfig, List<JedisShardInfo> shards) {
        super(poolConfig, null);
        this.factory = new RoundRobinFactory(shards);
        jedisClientPool = new GenericObjectPool(factory, poolConfig);
        jedisClientPool.setLifo(false);

        initializePool();
    }

    public RoundRobinPool(final GenericObjectPool.Config poolConfig, PoolableObjectFactory factory) {
        super(poolConfig, factory);
    }

    private void initializePool() {
        for (int i = factory.getNbShards(); i > 0; i--) {
            try {
                jedisClientPool.addObject();
            } catch (Exception e) {
                throw new IllegalStateException("Pb initialisation pool jedis", e);
            }
        }
    }

    public void setWhenExhaustedGrow(boolean whenExhaustedGrow) {
        this.jedisClientPool.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_GROW);
    }

    public void setMinIdle(int minIdle) {
        jedisClientPool.setMinIdle(minIdle);
    }

    public void setMaxIdle(int maxIdle) {
        jedisClientPool.setMaxIdle(maxIdle);
    }

    public Jedis getResource() {
        try {
            return (Jedis) jedisClientPool.borrowObject();
        } catch (Exception e) {
            throw new JedisConnectionException("Could not get a resource from the pool", e);
        }
    }

    public RoundRobinPool chainGetResource(Jedis result) {
        try {
            result = (Jedis) jedisClientPool.borrowObject();
        } catch (Exception e) {
            throw new JedisConnectionException("Could not get a resource from the pool", e);
        }
        return this;
    }

    public void returnResource(final Jedis resource) {
        try {
            jedisClientPool.returnObject(resource);
        } catch (Exception e) {
            throw new JedisException("Could not return the resource to the pool", e);
        }
    }

    public void chainReturnResource(final Jedis resource, Pool pool) {
        try {
            jedisClientPool.returnObject(resource);
        } catch (Exception e) {
            throw new JedisException("Could not return the resource to the pool", e);
        }
    }

    public void returnBrokenResource(final Jedis resource) {
        try {
            jedisClientPool.invalidateObject(resource);
        } catch (Exception e) {
            throw new JedisException("Could not return the resource to the pool", e);
        }
    }

    public void destroy() {
        try {
            jedisClientPool.close();
        } catch (Exception e) {
            throw new JedisException("Could not destroy the pool", e);
        }
    }

    public void setTestOnBorrow(boolean stob) {
        jedisClientPool.setTestOnBorrow(stob);
    }

    public void setTestOnReturn(boolean stor) {
        jedisClientPool.setTestOnReturn(stor);
    }

    /**
     * Factory du pool
     */

    private static class RoundRobinFactory extends BasePoolableObjectFactory {
        private List<JedisShardInfo> shards;
        private Iterator<JedisShardInfo> shardIterator;

        public RoundRobinFactory(List<JedisShardInfo> shards) {
            this.shards = shards;
            this.shardIterator = this.shards.iterator();
        }

        public Object makeObject() throws Exception {
            JedisShardInfo jsi = null;
            if (shardIterator.hasNext()) {
                jsi = (JedisShardInfo) shardIterator.next();
            } else {
                resetShardsIterator();
                if (shardIterator.hasNext()) {
                    jsi = (JedisShardInfo) shardIterator.next();
                }
            }
            
            Jedis jedis = new Jedis(jsi.getHost(), jsi.getPort());
            jedis.connect();

            if (!StringUtils.isEmpty(jsi.getPassword())) {
                jedis.auth(jsi.getPassword());
            }

            return jedis;
        }

        public void destroyObject(final Object obj) throws Exception {
            if ((obj != null) && (obj instanceof Jedis)) {
                Jedis jedis = (Jedis) obj;
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

        public boolean validateObject(final Object obj) {
            try {
                Jedis jedis = ((Jedis) obj);
                if (!jedis.ping().equals("PONG")) {
                    return false;
                }
                return true;
            } catch (Exception ex) {
                return false;
            }
        }

        public int getNbShards() {
            return shards.size();
        }

        public void resetShardsIterator() {
            shardIterator = shards.iterator();
        }
    }
}