package redis.clients.rejis;

public interface IRejis {

    String get(String key);

    String set(String key, String value);

    String setex(String key, int timeout, String value);

    Long del(String... keys);

    Boolean exists(String key);

    void destroyPools();

    Long expire(String key, int timeout);
}
