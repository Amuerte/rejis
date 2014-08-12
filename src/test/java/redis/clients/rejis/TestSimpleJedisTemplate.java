package redis.clients.rejis;

import static org.easymock.EasyMock.anyObject;
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
import org.easymock.IMockBuilder;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

@SuppressWarnings("unchecked")
public class TestSimpleJedisTemplate implements IJedisTestConstants {

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

    private SimpleJedisTemplate jedis;

    /* Mocks */
    Pool<Jedis> mockPool = createMock(Pool.class);
    Jedis mockJedis = createStrictMock(Jedis.class);

    @Before
    public void setUp() {
        jedis = new SimpleJedisTemplate();
        jedis.setWritePool(mockPool);
        jedis.setReadPool(mockPool);
    }

    @Test
    public void testSimpleMasterMode() throws Exception {
        jedis = new SimpleJedisTemplate(new JedisPoolConfig(), "127.0.0.1", 6379, null);

        assertThat(jedis.getWritePool(), is(jedis.getReadPool()));
    }

    @Test
    public void testDoPut_JedisLostConnection_SocketTimeout() throws Exception {
        // given
        expect(mockPool.getResource()).andReturn(mockJedis).once();
        expect(mockJedis.isConnected()).andReturn(false).once();
        mockJedis.connect();
        expectLastCall();
        mockPool.returnResource(mockJedis);
        expectLastCall();

        replayAll();

        // when
        Object result = jedis.doPut(SUCCESS_CALLBACK);

        // then
        assertThat(result, is(SUCCESS));
        verifyAll();
    }

    @Test
    public void testDoPut_JedisLostConnection_RedisDown() throws Exception {
        // given
        expect(mockPool.getResource()).andReturn(mockJedis).once();
        expect(mockJedis.isConnected()).andReturn(false).once();
        mockJedis.connect();
        expectLastCall().andThrow(CONNEXION_REDIS_IMPOSSIBLE);
        mockPool.returnBrokenResource(mockJedis);
        expectLastCall();

        replayAll();

        // when
        try {
            jedis.doPut(SUCCESS_CALLBACK);
            fail();
        } catch (JedisConnectionException e) {
            verifyAll();
        }
    }

    @Test
    public void testDoGet_JedisLostConnection_SocketTimeout() throws Exception {
        // given
        expect(mockPool.getResource()).andReturn(mockJedis).once();
        expect(mockJedis.isConnected()).andReturn(false).once();
        mockJedis.connect();
        expectLastCall();
        mockPool.returnResource(mockJedis);
        expectLastCall();

        replayAll();

        // when
        Object result = jedis.doGet(SUCCESS_CALLBACK);

        // then
        assertThat(result, is(SUCCESS));
        verifyAll();
    }

    @Test
    public void testDoGet_JedisLostConnection_RedisDown() throws Exception {
        // given
        expect(mockPool.getResource()).andReturn(mockJedis).once();
        expect(mockJedis.isConnected()).andReturn(false).once();
        mockJedis.connect();
        expectLastCall().andThrow(CONNEXION_REDIS_IMPOSSIBLE);
        mockPool.returnBrokenResource(mockJedis);
        expectLastCall();

        replayAll();

        // when
        try {
            jedis.doGet(SUCCESS_CALLBACK);
            fail();
        } catch (JedisConnectionException e) {
            verifyAll();
        }
    }



    @Test
    public void testMethod_Del_Callback() throws Exception {
        // given
        expect(mockPool.getResource()).andReturn(mockJedis).once();
        mockPool.returnResource(mockJedis);
        expectLastCall();

        expect(mockJedis.isConnected()).andReturn(true).once();
        expect(mockJedis.del(KEY)).andReturn(JEDIS_LONG_OK).once();
        replayAll();

        // when
        long retour = jedis.del(KEY);

        // then
        assertThat(retour, is(JEDIS_LONG_OK));
        verifyAll();
    }

    @Test
    public void testMethod_Del_callDoPut() throws Exception {
        // given
        SimpleJedisTemplate spyJedisTemplate = createSpyJedisTemplate();
        expect(spyJedisTemplate.doPut(anyObject(JedisCallback.class))).andReturn(JEDIS_LONG_OK).once();
        replay(spyJedisTemplate);

        // when
        spyJedisTemplate.del(KEY);

        // then
        verify(spyJedisTemplate);
    }

    SimpleJedisTemplate createSpyJedisTemplate() {
        IMockBuilder<SimpleJedisTemplate> mockBuilder = EasyMock.createMockBuilder(SimpleJedisTemplate.class);
        mockBuilder.addMockedMethod("doPut");
        mockBuilder.addMockedMethod("doGet");
        SimpleJedisTemplate spyJedisTemplate = mockBuilder.createMock();

        return spyJedisTemplate;
    }

    private void replayAll() {
        replay(mockJedis, mockPool);
    }

    void verifyAll() {
        verify(mockJedis, mockPool);
    }
}
