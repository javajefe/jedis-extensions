package tech.javajefe.redis.jedis.extensions;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.commands.ScriptingCommands;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by BukarevAA on 24.02.2019.
 */
public class RedisExtensions {

    private static final Logger log = LoggerFactory.getLogger(RedisExtensions.class);
    private enum SCRIPTS {

        executeArbitraryCommand("/lua/executeArbitraryCommand.lua"),
        batchXADD("/lua/batchXADD.lua");

        private String path;

        SCRIPTS(String path) {
            this.path = path;
        }

        String getPath() {
            return this.path;
        }
    }
    private final Gson gson;
    private final ScriptingCommands client;
    private final Map<SCRIPTS, String> loadedScriptIds;

    public RedisExtensions(ScriptingCommands client) throws IOException {
        this.gson = new Gson();
        this.client = client;
        Map<SCRIPTS, String> scripts = new HashMap<>();
        for (SCRIPTS scriptMeta: SCRIPTS.values()) {
            String lua = Utils.loadResource(scriptMeta.getPath());
            String sha = client.scriptLoad(lua);
            log.debug("Loaded script {} with sha marker {}", sha);
            scripts.put(scriptMeta, sha);
        }
        this.loadedScriptIds = Collections.unmodifiableMap(scripts);
    }

    public List<StreamMessageId> batchXADD(String key, List<Map<String, String>> messages) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is required");
        }
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("messages are required");
        }
        if (messages.stream().flatMap(m -> m.values().stream()).anyMatch(v -> v == null)) {
            throw new IllegalArgumentException("null values are disallowed");
        }
        String sha = loadedScriptIds.get(SCRIPTS.batchXADD);
        List<String> argv = messages.stream()
                .map(m -> gson.toJson(m))
                .collect(Collectors.toList());
        List<String> ids = (List<String>) client.evalsha(sha, Collections.singletonList(key), argv);
        return ids.stream().map(StreamMessageId::from).collect(Collectors.toList());
    }

    public Object executeArbitraryCommand(String...params) {
        String sha = loadedScriptIds.get(SCRIPTS.executeArbitraryCommand);
        return client.evalsha(sha, 0, params);
    }

    public Object executeArbitraryCommand(List<String> params) {
        String sha = loadedScriptIds.get(SCRIPTS.executeArbitraryCommand);
        return client.evalsha(sha, Collections.EMPTY_LIST, params);
    }

    public long XDEL(String key, List<StreamMessageId> ids) {
        // TODO
        List<String> params = ids.stream()
                .map(StreamMessageId::toString)
                .collect(Collectors.toList());
        params.add(0, key);
        params.add(0, "XDEL");
        return (Long) executeArbitraryCommand(params);
    }

    public long XLEN(String key) {
        // TODO
        return (Long) executeArbitraryCommand("XLEN", key);
    }

    private Map<StreamMessageId, Map<String, String>> parseRangeResult(Object result) {
        Map<StreamMessageId, Map<String, String>> range = new LinkedHashMap<>();
        for (Object el: (List) result) {
            List l = (List) el;
            StreamMessageId smid = StreamMessageId.from((String) l.get(0));
            List<String> m = (List<String>) l.get(1);
            Map<String, String> message = new HashMap<>();
            for (int i = 0; i < m.size(); i += 2) {
                message.put(m.get(i), m.get(i + 1));
            }
            range.put(smid, message);
        }
        return range;
    }

    public Map<StreamMessageId, Map<String, String>> XRANGE(String key) {
        return XRANGE(key, StreamMessageId.MIN, StreamMessageId.MAX);
    }

    public Map<StreamMessageId, Map<String, String>> XRANGE(String key, StreamMessageId start, StreamMessageId end) {
        // TODO
        return parseRangeResult(executeArbitraryCommand("XRANGE", key,
                start.toString(), end.toString()));
    }

    public Map<StreamMessageId, Map<String, String>> XRANGE(String key, StreamMessageId start, StreamMessageId end, int count) {
        // TODO
        return parseRangeResult(executeArbitraryCommand("XRANGE", key,
                start.toString(), end.toString(), "COUNT", Integer.toString(count)));
    }

    public Map<StreamMessageId, Map<String, String>> XREVRANGE(String key) {
        return XRANGE(key, StreamMessageId.MAX, StreamMessageId.MIN);
    }

    public Map<StreamMessageId, Map<String, String>> XREVRANGE(String key, StreamMessageId end, StreamMessageId start) {
        // TODO
        return parseRangeResult(executeArbitraryCommand("XREVRANGE", key,
                end.toString(), start.toString()));
    }

    public Map<StreamMessageId, Map<String, String>> XREVRANGE(String key, StreamMessageId end, StreamMessageId start, int count) {
        // TODO
        return parseRangeResult(executeArbitraryCommand("XREVRANGE", key,
                end.toString(), start.toString(), "COUNT", Integer.toString(count)));
    }
}
