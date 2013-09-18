package redis.clients.rejis;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.rejis.JedisCallback;
import redis.clients.rejis.RoundRobinJedisTemplate;
import redis.clients.util.Pool;

@SuppressWarnings("unchecked")
public class TestRoundRobinJedisTemplate implements IJedisTestConstants {

    static final JedisCallback<Jedis, Object> SUCCESS_CALLBACK = new JedisCallback<Jedis, Object>() {
        @Override
        public Object doJedisAction(Jedis jedis) {
            return SUCCESS;
        }
    };

    static final JedisCallback<Jedis, Object> ERROR_CALLBACK = new JedisCallback<Jedis, Object>() {
        @Override
        public Object doJedisAction(Jedis jedis) {
            throw JEDIS_ACTION_FAILED;
        }
    };

    private RoundRobinJedisTemplate jedis;

    /* Mocks */
    Pool<Jedis> readPool = createMock("readPool", Pool.class);
    Pool<Jedis> writePool = createMock("writePool", Pool.class);
    Jedis mockJedis = createStrictMock(Jedis.class);

    @Before
    public void setUp() {
        jedis = new RoundRobinJedisTemplate();
        jedis.setWritePool(writePool);
        jedis.setReadPool(readPool);
    }

    @Test
    public void testDoGet_Recursif_SocketTimeout_Slave_Lecture_Sur_Master() throws Exception {
        // given
        final int nbSlave = jedis.getRedundancyFactor();
        EasyMock.resetToDefault(mockJedis);

        expect(readPool.getResource()).andReturn(mockJedis).times(nbSlave);
        expect(writePool.getResource()).andReturn(mockJedis).once();
        expect(mockJedis.isConnected()).andReturn(false).times(nbSlave).andReturn(true).once();
        mockJedis.connect();
        expectLastCall().andThrow(CONNEXION_REDIS_IMPOSSIBLE).times(nbSlave);
        readPool.returnBrokenResource(mockJedis);
        expectLastCall().times(nbSlave);
        writePool.returnResource(mockJedis);
        expectLastCall().once();

        replayAll();

        // when
        Object result = null;
        result = jedis.doGet(SUCCESS_CALLBACK, nbSlave);

        // then
        assertThat(result, is(SUCCESS));
        verifyAll();
    }

    @Test
    public void testDoGet_Recursif_SocketTimeout_Et_Master() throws Exception {
        // given
        final int nbSlave = jedis.getRedundancyFactor();
        EasyMock.resetToDefault(mockJedis);
        
        expect(readPool.getResource()).andReturn(mockJedis).times(nbSlave);
        expect(writePool.getResource()).andReturn(mockJedis).once();
        expect(mockJedis.isConnected()).andReturn(false).times(nbSlave + 1);
        mockJedis.connect();
        expectLastCall().andThrow(CONNEXION_REDIS_IMPOSSIBLE).times(nbSlave + 1);
        readPool.returnBrokenResource(mockJedis);
        expectLastCall().times(nbSlave);
        writePool.returnBrokenResource(mockJedis);
        expectLastCall();
        
        replayAll();

        // when
        try {
            jedis.doGet(SUCCESS_CALLBACK, nbSlave);
            fail();
        } catch (JedisConnectionException e) {
            verifyAll();
        }
    }

    @Test
    public void testDoGet_GetResourceFromPoolError_Slave_Lecture_Master() throws Exception {
        // given
        final int nbSlave = jedis.getRedundancyFactor();

        expect(readPool.getResource()).andThrow(CONNEXION_REDIS_IMPOSSIBLE).times(nbSlave);
        expect(writePool.getResource()).andReturn(mockJedis).once();
        writePool.returnResource(mockJedis);
        expectLastCall().once();
        expect(mockJedis.isConnected()).andReturn(true).once();

        replayAll();

        // when
        Object result = null;
        result = jedis.doGet(SUCCESS_CALLBACK, nbSlave);

        // then
        assertThat(result, is(SUCCESS));
        verifyAll();
    }

    @Test
    public void testDoGet_GetResourceFromPoolError_Slave_Et_Master() throws Exception {
        // given
        final int nbSlave = jedis.getRedundancyFactor();

        expect(readPool.getResource()).andThrow(CONNEXION_REDIS_IMPOSSIBLE).times(nbSlave);
        expect(writePool.getResource()).andThrow(CONNEXION_REDIS_IMPOSSIBLE).once();

        replayAll();

        // when
        try {
            jedis.doGet(SUCCESS_CALLBACK, nbSlave);
            fail();
        } catch (JedisConnectionException e) {
            verifyAll();
        }
    }

    @Test
    public void testDoGet_Read_Failed_On_Slaves_And_Succeed_On_Master() throws Exception {
        // given
        final int nbSlave = jedis.getRedundancyFactor();
        expect(readPool.getResource()).andReturn(mockJedis).times(nbSlave);
        readPool.returnBrokenResource(mockJedis);
        expectLastCall().times(nbSlave);
        expect(writePool.getResource()).andReturn(mockJedis).once();
        writePool.returnResource(mockJedis);
        expectLastCall().once();
        expect(mockJedis.isConnected()).andReturn(true).times(nbSlave + 1);

        JedisCallback<Jedis, Object> mockCallBack = createMock(JedisCallback.class);
        expect(mockCallBack.doJedisAction(mockJedis)).andThrow(JEDIS_ACTION_FAILED).times(nbSlave).andReturn(SUCCESS).once();
        replay(mockCallBack);

        replayAll();

        // when
        Object result = null;
        result = jedis.doGet(mockCallBack, nbSlave);

        // then
        assertThat(result, is(SUCCESS));
        verifyAll();
    }

    @Test
    public void testDoGet_Read_Failed_On_First_Slave() throws Exception {
        // given
        final int nbSlave = jedis.getRedundancyFactor();
        expect(readPool.getResource()).andReturn(mockJedis).times(nbSlave);
        readPool.returnBrokenResource(mockJedis);
        expectLastCall().once();
        readPool.returnResource(mockJedis);
        expectLastCall().once();
        expect(mockJedis.isConnected()).andReturn(true).times(nbSlave);

        JedisCallback<Jedis, Object> mockCallBack = createMock(JedisCallback.class);
        expect(mockCallBack.doJedisAction(mockJedis)).andThrow(JEDIS_ACTION_FAILED).once().andReturn(SUCCESS).once();
        replay(mockCallBack);

        replayAll();

        // when
        Object result = null;
        result = jedis.doGet(mockCallBack, nbSlave);

        // then
        assertThat(result, is(SUCCESS));
        verifyAll();
    }

    private void replayAll() {
        replay(mockJedis, readPool, writePool);
    }

    void verifyAll() {
        verify(mockJedis, readPool, writePool);
    }
}
