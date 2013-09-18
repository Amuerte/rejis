package redis.clients.rejis;

public interface JedisCallback<E, T> {

    T doJedisAction(E jedis);

}
