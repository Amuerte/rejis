package redis.clients.rejis;

import redis.clients.jedis.exceptions.JedisConnectionException;

public interface IJedisTestConstants {

    Object SUCCESS = new Object();
    String VALUE = "value";
    String KEY = "key";

    String JEDIS_STRING_OK = "OK";
    long JEDIS_LONG_OK = 0;

    JedisConnectionException CONNEXION_REDIS_IMPOSSIBLE = new JedisConnectionException("Connection a Redis impossible");
    JedisConnectionException JEDIS_ACTION_FAILED = new JedisConnectionException("Jedis action failed");
}
