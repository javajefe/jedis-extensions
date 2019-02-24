package org.javajefe.redis.jedis.extensions

import org.testcontainers.containers.GenericContainer
import org.testcontainers.spock.Testcontainers
import redis.clients.jedis.Jedis
import spock.lang.Shared
import spock.lang.Specification

/**
 * Created by BukarevAA on 18.02.2019.
 */
@Testcontainers
class RedisExtensionsTests extends Specification {

    @Shared
    GenericContainer redis = new GenericContainer("redis:5.0.3")
            .withExposedPorts(6379)
    Jedis jedis
    RedisExtensions redisExtensions
    def streamName = 'TEST:JEDIS:STREAM'

    def setup() {
        // Instantiate the client
        jedis = new Jedis(redis.getContainerIpAddress(), redis.getMappedPort(6379))
        jedis.select(2)
        redisExtensions = new RedisExtensions(jedis)
        // To create stream we have to add something into it
        // But we want to keep it Clear
        redisExtensions.XDEL(streamName, redisExtensions.batchXADD(streamName, [['dummy': 'dummy']]))
    }

    def cleanup() {
        jedis.flushDB()
        jedis.close()
    }

    def "Empty key is not allowed in Batch XADD"() {
        when:
            redisExtensions.batchXADD('', [[k: 'v']])
        then:
            IllegalArgumentException e = thrown()
    }

    def "Empty message list is not allowed in Batch XADD"() {
        when:
            redisExtensions.batchXADD(streamName, [])
        then:
            IllegalArgumentException e = thrown()
    }

    def "Batch XADD for 1000 messages"() {
        setup:
            def streamSize = redisExtensions.XLEN(streamName)
        when:
            redisExtensions.batchXADD(streamName, (1..1000).collect {[i: it as String]})
            streamSize = redisExtensions.XLEN(streamName)
        then:
            streamSize == old(streamSize) + 1000
    }

    def "Batch XADD does not accept null values"() {
        when:
            redisExtensions.batchXADD(streamName, [[k: null]])
        then:
            IllegalArgumentException e = thrown()
    }

    def "Batch XADD checks all messages before execution"() {
        when:
            def streamSize = redisExtensions.XLEN(streamName)
            redisExtensions.batchXADD(streamName, [[k: 'v'], [k: null]])
        then:
            IllegalArgumentException e = thrown()
            redisExtensions.XLEN(streamName) == streamSize

    }

    def "Batch XADD should process UTF-8 symbols correctly"() {
        setup:
            def nonASCII = 'Простой тест'
        when:
            redisExtensions.batchXADD(streamName, [['s': nonASCII]])
            def range = redisExtensions.XREVRANGE(streamName, StreamMessageId.MAX, StreamMessageId.MIN, 1)
        then:
            range
            range.size() == 1
            range.values().any {it.s == nonASCII}
    }

    def "XDEL deletes messages"() {
        setup:
            def ids = redisExtensions.batchXADD(streamName, (1..10).collect {[i: it as String]})
            def streamSize = redisExtensions.XLEN(streamName)
        when:
            def deleted = redisExtensions.XDEL(streamName, [ids[1], ids[3], ids[5]])
            streamSize = redisExtensions.XLEN(streamName)
        then:
            deleted == 3
            streamSize == old(streamSize) - deleted
    }

    def "XREVRANGE is just the reverse of XRANGE"() {
        when:
            def ids = redisExtensions.batchXADD(streamName, (1..10).collect {[i: it as String]})
            def res = redisExtensions.XRANGE(streamName, StreamMessageId.MIN, StreamMessageId.MAX, ids.size())
            def revres = redisExtensions.XREVRANGE(streamName, StreamMessageId.MAX, StreamMessageId.MIN, ids.size())
        then:
            ids.size() == res.size()
            res.keySet() == revres.keySet()
    }

    def "XRANGE without COUNT returns the whole stream"() {
        when:
            def ids = redisExtensions.batchXADD(streamName, (1..10).collect {[i: it as String]})
            def range = redisExtensions.XRANGE(streamName)
        then:
            range.keySet()  == ids as Set
            range.values()*.i == (1..10).collect {it as String}
    }
}
