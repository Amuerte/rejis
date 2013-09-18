package redis.clients.rejis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

public class SimpleJedisTemplate extends AbstractJedisTemplate<Jedis, Jedis> {

    protected SimpleJedisTemplate() {
    }

    /**
     * Client Redis mono-instance (1 seul master utilisé pour la lecture et
     * l'ecriture).
     * 
     * @param jedisPoolConfig
     * @param masterIP
     * @param masterPort
     */
    public SimpleJedisTemplate(JedisPoolConfig jedisPoolConfig, String masterIP, int masterPort) {
        Pool<Jedis> pool = new JedisPool(jedisPoolConfig, masterIP, masterPort);
        this.setWritePool(pool);
        this.setReadPool(pool);
    }


    @Override
    public Long del(final String... keys) {
        return doPut(new JedisCallback<Jedis, Long>() {
            @Override
            public Long doJedisAction(Jedis jedis) {
                return jedis.del(keys);
            }
        });
    }

    @Override
    protected Jedis getJedisFromReadPool(Pool<Jedis> readPool) {
        Jedis jedis = super.getJedisFromReadPool(readPool);
        validClientConnection(readPool, jedis);

        return jedis;
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
