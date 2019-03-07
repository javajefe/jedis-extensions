package tech.javajefe.redis.jedis.extensions;

/**
 * Created by Alexander Bukarev on 07.03.2019.
 */
enum Scripts {

    executeArbitraryCommand("/lua/executeArbitraryCommand.lua"),
    batchXADD("/lua/batchXADD.lua");

    private String path;

    Scripts(String path) {
        this.path = path;
    }

    String getPath() {
        return this.path;
    }
}
