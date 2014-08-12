package redis.clients.rejis;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;

public class TestRoundRobinTemplateIT {

    private final static String HOST = "localhost";
    private final static Integer MASTER_PORT = 6379;
    private final static Integer SLAVE_1_PORT = 6389;
    private final static Integer SLAVE_2_PORT = 6399;
    private final static String[] SHARDS_CONFIG = new String[] { HOST + ":" + SLAVE_1_PORT, HOST + ":" + SLAVE_2_PORT };

    private final static int NB_KEYS = 300000;
    private final static String KEY_RADIC = "KEY_";

    private final static int NB_THREAD = 6;

    @Test
    public void testRoundRobinPool() throws Exception {
        final IRejis template = new TestRoundRoundJedisTemplate(buildPoolConfig(), HOST, MASTER_PORT, null, buildShardsList());
        writeDataToMaster(template);

        final List<Callable<String>> tasks = new ArrayList<Callable<String>>(1000);

        for (int i = 1; i <= NB_KEYS; i++) {
            final String key = KEY_RADIC + i;

            tasks.add(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return template.get(key);
                }
            });
        }

        final ExecutorService executorPool = Executors.newFixedThreadPool(NB_THREAD);
        final List<Future<String>> listResults = executorPool.invokeAll(tasks);

        final Map<Integer, List<Integer>> keyreadByHost = new HashMap<Integer, List<Integer>>();
        keyreadByHost.put(SLAVE_1_PORT, new ArrayList<Integer>());
        keyreadByHost.put(SLAVE_2_PORT, new ArrayList<Integer>());
        
        for (Future<String> future : listResults) {
            String result = future.get();
            String[] resultInfo = result.split("\\|");

            System.out.println("port: " + resultInfo[1] + " - value : " + resultInfo[0]);
            List<Integer> valuesReadByHost = keyreadByHost.get(Integer.valueOf(resultInfo[1]));
            valuesReadByHost.add(Integer.valueOf(resultInfo[0]));
        }

        final int nbKeyRetreivedOnSlave1 = keyreadByHost.get(SLAVE_1_PORT).size();
        final int nbKeyRetreivedOnSlave2 = keyreadByHost.get(SLAVE_2_PORT).size();

        assertThat(nbKeyRetreivedOnSlave1 + nbKeyRetreivedOnSlave2, is(NB_KEYS));

        int slave1Percent = calcPercentage(nbKeyRetreivedOnSlave1);
        int slave2Percent = calcPercentage(nbKeyRetreivedOnSlave2);
        
        System.out.println(String.format("Port %d: %d%% (%d)\nPort %d: %d%% (%d)\n", SLAVE_1_PORT, slave1Percent, nbKeyRetreivedOnSlave1, SLAVE_2_PORT, slave2Percent,
                nbKeyRetreivedOnSlave2));
    }

    void writeDataToMaster(IRejis canalJedis) {
        for (int i = 1; i <= NB_KEYS; i++) {
            canalJedis.set(KEY_RADIC + i, Integer.toString(i));
        }
    }

    JedisPoolConfig buildPoolConfig() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxWait(400);
        return config;
    }

    List<JedisShardInfo> buildShardsList() {
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        for (String shardConfig : SHARDS_CONFIG) {
            String[] shardInfo = shardConfig.split(":");
            JedisShardInfo jedisSharfInfo = new JedisShardInfo(shardInfo[0], Integer.valueOf(shardInfo[1]));
            shards.add(jedisSharfInfo);
        }

        return shards;
    }

    int calcPercentage(int nbElementProcessed) {
        return Math.round(((float) nbElementProcessed / NB_KEYS * 100));
    }

    /**
     * Inner class to retreive client port.
     * 
     * @author Benjamin VEZON
     */
    private class TestRoundRoundJedisTemplate extends RoundRobinJedisTemplate {

        public TestRoundRoundJedisTemplate(JedisPoolConfig jedisPoolConfig, String masterIP, int masterPort, String password, List<JedisShardInfo> shards) {
            super(jedisPoolConfig, masterIP, masterPort, password, shards);
        }

        @Override
        public String get(final String key) {
            return doGet(new JedisCallback<Jedis, String>() {
                @Override
                public String doJedisAction(Jedis jedis) {
                    return jedis.get(key) + "|" + jedis.getClient().getPort();
                }
            });
        }
    }
}
