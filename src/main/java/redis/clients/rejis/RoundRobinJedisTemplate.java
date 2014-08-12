package redis.clients.rejis;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

public class RoundRobinJedisTemplate extends SimpleJedisTemplate {

    // this also works as a switch between single node / local use of jedis:
    // redundancyFactor 0
    private int redundancyFactor = 2;

    protected RoundRobinJedisTemplate() {
    }

    public RoundRobinJedisTemplate(JedisPoolConfig jedisPoolConfig, String masterIP, int masterPort, String password, List<JedisShardInfo> shards) {
        super(jedisPoolConfig, masterIP, masterPort, password);
        setReadPool(new RoundRobinPool(new JedisPoolConfig(), shards));
        setRedundancyFactor(shards.size());
    }

    @Override
    protected <T> T doGet(JedisCallback<Jedis, T> jedisCallback) {
        return this.doGet(jedisCallback, getRedundancyFactor());
    }

    protected <T> T doGet(JedisCallback<Jedis, T> jedisCallback, int tryCounter) {
        T result = null;
        Jedis jedis = null;
        boolean isResourceInError = false;

        Pool<Jedis> pool = selectPool(tryCounter);
        try {
            jedis = getJedisFromReadPool(pool);
            result = jedisCallback.doJedisAction(jedis);
        } catch (JedisConnectionException ex) {
            isResourceInError = true;
            returnBrokenClientToPool(jedis, pool);

            if (tryCounter > 0) {
                result = doGet(jedisCallback, tryCounter - 1);
            } else {
                throw new JedisConnectionException("CanalJedis : Cant get value from redis Cache", ex);
            }
        } finally {
            if (!isResourceInError) {
                returnClientToPool(jedis, pool);
            }
        }

        return result;
    }

    protected Pool<Jedis> selectPool(int nbTry) {
        if (nbTry > 0) {
            return getReadPool();
        } else {
            return getWritePool();
        }
    }

    public int getRedundancyFactor() {
        return redundancyFactor;
    }

    public void setRedundancyFactor(int redundancyFactor) {
        this.redundancyFactor = redundancyFactor;
    }
}
