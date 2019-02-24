package org.javajefe.redis.jedis.extensions;

/**
 * Created by BukarevAA on 24.02.2019.
 */
public class StreamMessageId {

    public static final StreamMessageId MAX = new StreamMessageId("+");
    public static final StreamMessageId MIN = new StreamMessageId("-");
    private final long timestamp;
    private final long counter;
    private final String string;

    private StreamMessageId(String string) {
        timestamp = 0;
        counter = 0;
        this.string = string;
    }

    public StreamMessageId(long timestamp, long counter) {
        this.timestamp = timestamp;
        this.counter = counter;
        this.string = timestamp + "-" + counter;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getCounter() {
        return counter;
    }

    @Override
    public String toString() {
        return string;
    }

    public StreamMessageId next() {
        return new StreamMessageId(timestamp, counter + 1);
    }

    public static StreamMessageId from(String streamMessageId) {
        if (MAX.toString().equals(streamMessageId)) {
            return MAX;
        } else if (MIN.toString().equals(streamMessageId)) {
            return MIN;
        }
        String[] smid = streamMessageId.split("-");
        return new StreamMessageId(Long.parseLong(smid[0]), Long.parseLong(smid[1]));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StreamMessageId that = (StreamMessageId) o;

        return string.equals(that.string);
    }

    @Override
    public int hashCode() {
        return string.hashCode();
    }
}
