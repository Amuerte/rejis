package redis.clients.rejis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

public abstract class AbstractJedisTemplate<R extends BinaryJedisCommands & JedisCommands, W extends BinaryJedisCommands & JedisCommands> implements IRejis {

    private static final Logger logger = LoggerFactory.getLogger(AbstractJedisTemplate.class);

    private Pool<W> writePool;
    private Pool<R> readPool;

    @Override
    public String get(final String key) {
        return doGet(new JedisCallback<R, String>() {
            @Override
            public String doJedisAction(R jedis) {
                return jedis.get(key);
            }
        });
    }

    @Override
    public String set(final String key, final String value) {
        return doPut(new JedisCallback<W, String>() {
            @Override
            public String doJedisAction(W jedis) {
                return jedis.set(key, value);
            }
        });
    }

    @Override
    public String setex(final String key, final int timeout, final String value) {
        return doPut(new JedisCallback<W, String>() {
            @Override
            public String doJedisAction(W jedis) {
                return jedis.setex(key, timeout, value);
            }
        });
    }


    @Override
    public Boolean exists(final String key) {
        return doGet(new JedisCallback<R, Boolean>() {
            @Override
            public Boolean doJedisAction(R jedis) {
                return jedis.exists(key);
            }
        });
    }

    // TODO : manage JedisException ?
    @Override
    public void destroyPools() {
        if (writePool != null) {
            writePool.destroy();
        }

        if (readPool != null) {
            readPool.destroy();
        }
    }

    protected <T> T doPut(JedisCallback<W, T> jedisCallback) {
        boolean isResourceInError = false;
        W jedis = null;
        T result = null;
        try {
            jedis = getJedisFromWritePool(writePool);
            result = jedisCallback.doJedisAction(jedis);
        } catch (JedisConnectionException e) {
            isResourceInError = true;
            returnBrokenClientToPool(jedis, writePool);
            throw e;
        } finally {
            if (!isResourceInError) {
                returnClientToPool(jedis, writePool);
            }
        }

        return result;
    }

    protected <T> T doGet(JedisCallback<R, T> jedisCallback) {
        T result = null;
        R jedis = null;
        boolean isResourceInError = false;

        try {
            jedis = getJedisFromReadPool(readPool);
            result = jedisCallback.doJedisAction(jedis);
        } catch (JedisConnectionException e) {
            isResourceInError = true;
            returnBrokenClientToPool(jedis, readPool);
            throw e;
        } finally {
            if (!isResourceInError) {
                returnClientToPool(jedis, readPool);
            }
        }

        return result;
    }

    protected <J extends BinaryJedisCommands & JedisCommands> J getJedisFromPool(Pool<J> pool) {
        J jedis = pool.getResource();

        return jedis;
    }

    protected W getJedisFromWritePool(Pool<W> writePool) {
        return getJedisFromPool(writePool);
    }

    protected R getJedisFromReadPool(Pool<R> readPool) {
        return getJedisFromPool(readPool);
    }

    protected <J extends BinaryJedisCommands & JedisCommands> void returnClientToPool(J jedis, Pool<J> pool) {
        if (jedis == null || pool == null) {
            return;
        }

        try {
            pool.returnResource(jedis);
        } catch (JedisException e) {
            logger.debug("Cannot return jedis client to pool", e);
        }
    }

    protected <J extends BinaryJedisCommands & JedisCommands> void returnBrokenClientToPool(J jedis, Pool<J> pool) {
        if (jedis == null || pool == null) {
            return;
        }

        try {
            pool.returnBrokenResource(jedis);
        } catch (JedisException e) {
            logger.debug("Cannot return broken jedis client to pool", e);
        }
    }

    protected void setWritePool(Pool<W> writePool) {
        this.writePool = writePool;
    }

    protected void setReadPool(Pool<R> readPool) {
        this.readPool = readPool;
    }

    public Pool<W> getWritePool() {
        return writePool;
    }

    public Pool<R> getReadPool() {
        return readPool;
    }
}
