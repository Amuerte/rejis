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

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

@SuppressWarnings("unchecked")
public class TestAbstractJedisTemplate implements IJedisTestConstants {

    static final JedisCallback<JedisClientTest, Object> SUCCESS_CALLBACK = new JedisCallback<JedisClientTest, Object>() {
        @Override
        public Object doJedisAction(JedisClientTest jedis) {
            return SUCCESS;
        }
    };

    static final JedisCallback<JedisClientTest, Object> ERROR_CALLBACK = new JedisCallback<JedisClientTest, Object>() {
        @Override
        public Object doJedisAction(JedisClientTest jedis) {
            throw JEDIS_ACTION_FAILED;
        }
    };

    private AbstractJedisTemplate<JedisClientTest, JedisClientTest> jedis;

    /* Mocks */
    Pool<JedisClientTest> readPool = createMock("readPool", Pool.class);
    Pool<JedisClientTest> writePool = createMock("writePool", Pool.class);
    JedisClientTest mockJedis = createStrictMock(JedisClientTest.class);

    @Before
    public void setUp() {
        jedis = new JedisTemplateTest();
        jedis.setWritePool(writePool);
        jedis.setReadPool(readPool);
    }

    /* core tests */

    @Test
    public void testDoPut_CantGetResourceFromMaster() throws Exception {
        // given
        expect(writePool.getResource()).andThrow(CONNEXION_REDIS_IMPOSSIBLE);
        replayAll();

        // when
        try {
            jedis.doPut(SUCCESS_CALLBACK);
            fail();
        } catch (JedisConnectionException e) {
            // then
            verifyAll();
        }
    }

    @Test
    public void testDoPut_OK() throws Exception {
        // given
        expect(writePool.getResource()).andReturn(mockJedis).once();
        writePool.returnResource(mockJedis);
        expectLastCall();

        replayAll();

        // when
        Object result = jedis.doPut(SUCCESS_CALLBACK);

        // then
        assertThat(result, is(SUCCESS));
        verifyAll();
    }

    @Test
    public void testDoPut_CantPerformJedisAction() throws Exception {
        // given
        expect(writePool.getResource()).andReturn(mockJedis).once();
        writePool.returnBrokenResource(mockJedis);
        expectLastCall();

        replayAll();

        // when
        try {
            jedis.doPut(ERROR_CALLBACK);
            fail();
        } catch (JedisConnectionException e) {
            verifyAll();
        }
    }

    @Test
    public void testDoGet_CantConnectToMaster() throws Exception {
        // given
        expect(readPool.getResource()).andThrow(CONNEXION_REDIS_IMPOSSIBLE);
        replayAll();

        // when
        try {
            jedis.doGet(SUCCESS_CALLBACK);
            fail();
        } catch (JedisConnectionException e) {
            // then
            verifyAll();
        }
    }

    @Test
    public void testDoGet_OK() throws Exception {
        // given
        expect(readPool.getResource()).andReturn(mockJedis).once();
        readPool.returnResource(mockJedis);
        expectLastCall();

        replayAll();

        // when
        Object result = jedis.doGet(SUCCESS_CALLBACK);

        // then
        assertThat(result, is(SUCCESS));
        verifyAll();
    }

    @Test
    public void testDoGet_CantPerformJedisAction() throws Exception {
        // given
        expect(readPool.getResource()).andReturn(mockJedis).once();
        readPool.returnBrokenResource(mockJedis);
        expectLastCall();

        replayAll();

        // when
        try {
            jedis.doGet(ERROR_CALLBACK);
            fail();
        } catch (JedisConnectionException e) {
            verifyAll();
        }
    }

    /* END core tests */

    @Test
    public void testMethod_Set_Callback() throws Exception {
        // given
        expect(writePool.getResource()).andReturn(mockJedis).once();
        writePool.returnResource(mockJedis);
        expectLastCall();

        expect(mockJedis.set(KEY, VALUE)).andReturn(JEDIS_STRING_OK).once();
        replayAll();

        // when
        String retour = jedis.set(KEY, VALUE);

        // then
        assertThat(retour, is(JEDIS_STRING_OK));
        verifyAll();
    }

    @Test
    public void testMethod_Set_callDoPut() throws Exception {
        // given
        JedisTemplateTest spyJedisTemplate = createSpyJedisTemplate();
        expect(spyJedisTemplate.doPut(anyObject(JedisCallback.class))).andReturn(JEDIS_STRING_OK).once();
        replay(spyJedisTemplate);

        // when
        spyJedisTemplate.set(KEY, VALUE);

        // then
        verify(spyJedisTemplate);
    }

    @Test
    public void testMethod_Setex_Callback() throws Exception {
        // given
        final int timeout = 100;

        expect(writePool.getResource()).andReturn(mockJedis).once();
        writePool.returnResource(mockJedis);
        expectLastCall();

        expect(mockJedis.setex(KEY, timeout, VALUE)).andReturn(JEDIS_STRING_OK).once();
        replayAll();

        // when
        String retour = jedis.setex(KEY, timeout, VALUE);

        // then
        assertThat(retour, is(JEDIS_STRING_OK));
        verifyAll();
    }

    @Test
    public void testMethod_Setex_callDoPut() throws Exception {
        // given
        JedisTemplateTest spyJedisTemplate = createSpyJedisTemplate();
        expect(spyJedisTemplate.doPut(anyObject(JedisCallback.class))).andReturn(JEDIS_STRING_OK).once();
        replay(spyJedisTemplate);

        // when
        spyJedisTemplate.setex(KEY, 100, VALUE);

        // then
        verify(spyJedisTemplate);
    }

    @Test
    public void testMethod_Get_Callback() throws Exception {
        // given
        expect(readPool.getResource()).andReturn(mockJedis).once();
        expect(mockJedis.get(KEY)).andReturn(VALUE).once();
        readPool.returnResource(mockJedis);
        expectLastCall();
        replayAll();

        // when
        String retour = jedis.get(KEY);

        // then
        assertThat(retour, is(VALUE));
        verifyAll();
    }

    @Test
    public void testMethod_Get_callDoGet() throws Exception {
        // given
        JedisTemplateTest spyJedisTemplate = createSpyJedisTemplate();
        expect(spyJedisTemplate.doGet(anyObject(JedisCallback.class))).andReturn(VALUE).once();
        replay(spyJedisTemplate);

        // when
        spyJedisTemplate.get(KEY);

        // then
        verify(spyJedisTemplate);
    }

    @Test
    public void testMethod_Exists_Callback() throws Exception {
        // given
        expect(readPool.getResource()).andReturn(mockJedis).once();
        readPool.returnResource(mockJedis);
        expectLastCall();

        expect(mockJedis.exists(KEY)).andReturn(true).once();
        replayAll();

        // when
        boolean retour = jedis.exists(KEY);

        // then
        assertThat(retour, is(true));
        verifyAll();
    }

    @Test
    public void testMethod_Exists_callDoGet() throws Exception {
        // given
        JedisTemplateTest spyJedisTemplate = createSpyJedisTemplate();
        expect(spyJedisTemplate.doGet(anyObject(JedisCallback.class))).andReturn(true).once();
        replay(spyJedisTemplate);

        // when
        spyJedisTemplate.exists(KEY);

        // then
        verify(spyJedisTemplate);
    }

    @Test
    public void testMethod_Expire_Callback() throws Exception {
        // given
        final int timeout = 100;
        expect(writePool.getResource()).andReturn(mockJedis).once();
        writePool.returnResource(mockJedis);
        expectLastCall();

        expect(mockJedis.expire(KEY, timeout)).andReturn(JEDIS_LONG_OK).once();
        replayAll();

        // when
        long retour = jedis.expire(KEY, timeout);

        // then
        assertThat(retour, is(JEDIS_LONG_OK));
        verifyAll();
    }

    @Test
    public void testMethod_Expire_callDoPut() throws Exception {
        // given
        JedisTemplateTest spyJedisTemplate = createSpyJedisTemplate();
        expect(spyJedisTemplate.doPut(anyObject(JedisCallback.class))).andReturn(JEDIS_LONG_OK).once();
        replay(spyJedisTemplate);

        // when
        spyJedisTemplate.expire(KEY, 100);

        // then
        verify(spyJedisTemplate);
    }

    @Test
    public void testDestroyPools() throws Exception {
        // given
        readPool.destroy();
        expectLastCall();
        writePool.destroy();
        expectLastCall();
        replayAll();

        // when
        jedis.destroyPools();

        // then
        verifyAll();
    }

    @Test
    public void testDestroyPools_SingleMasterMode_NullPool() throws Exception {
        // given
        jedis.setWritePool(null);
        jedis.setReadPool(null);
        replayAll();

        // when
        jedis.destroyPools();

        // then
        verifyAll();
    }

    JedisTemplateTest createSpyJedisTemplate() {
        IMockBuilder<JedisTemplateTest> mockBuilder = EasyMock.createMockBuilder(JedisTemplateTest.class);
        mockBuilder.addMockedMethod("doPut");
        mockBuilder.addMockedMethod("doGet");
        JedisTemplateTest spyJedisTemplate = mockBuilder.createMock();

        return spyJedisTemplate;
    }

    private void replayAll() {
        replay(mockJedis, readPool, writePool);
    }

    void verifyAll() {
        verify(mockJedis, readPool, writePool);
    }

    private abstract class JedisClientTest implements BinaryJedisCommands, JedisCommands {

    }

    private class JedisTemplateTest extends AbstractJedisTemplate<JedisClientTest, JedisClientTest> {
        public Long del(String... keys) {
            throw new UnsupportedOperationException();
        }
    }
}
