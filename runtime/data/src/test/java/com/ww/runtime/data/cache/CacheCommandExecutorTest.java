package com.ww.runtime.data.cache;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** 统一命令执行器：key 位置、容器/Stream 语义、MSET 跨分区拒绝与统计命令。 */
@DisplayName("CacheCommandExecutor P0")
class CacheCommandExecutorTest {

    private static final NodeId SELF = NodeId.of("node-1");

    private CacheCapability capability;
    private CacheCommandExecutor executor;

    @BeforeEach
    void setUp() throws Exception {
        PlacementView view = new PlacementView() {
            @Override
            public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
                return Optional.of(new Owner(SELF, 3L));
            }
        };
        capability = new CacheCapability(SELF, view, key -> key.charAt(0) - 'a', false, null);
        capability.start();
        executor = new CacheCommandExecutor(capability);
    }

    @AfterEach
    void tearDown() {
        capability.stop();
    }

    private CacheCommandResponse exec(String command, Object... args) {
        return executor.execute(CacheCommandRequest.of(command, args));
    }

    private Object ok(String command, Object... args) {
        CacheCommandResponse response = exec(command, args);
        assertThat(response.ok()).as(command + " -> " + response.message()).isTrue();
        return response.result();
    }

    @Test
    @DisplayName("Hash/List/Set/ZSet 命令经统一通道可用")
    @SuppressWarnings("unchecked")
    void containerCommands() {
        assertThat(ok("HSET", "a:h", "f1", "v1", "f2", "v2")).isEqualTo(2L);
        assertThat(ok("HGET", "a:h", "f1")).isEqualTo("v1");
        assertThat(ok("HMGET", "a:h", "f1", "f2")).isEqualTo(List.of("v1", "v2"));
        assertThat((Map<String, Object>) ok("HGETALL", "a:h"))
                .containsEntry("f1", "v1").containsEntry("f2", "v2");
        assertThat(ok("HLEN", "a:h")).isEqualTo(2L);
        assertThat(ok("HEXISTS", "a:h", "f1")).isEqualTo(true);
        assertThat(ok("HDEL", "a:h", "f1")).isEqualTo(1L);

        assertThat(ok("RPUSH", "a:l", "x", "y", "z")).isEqualTo(3L);
        assertThat(ok("LLEN", "a:l")).isEqualTo(3L);
        assertThat(ok("LRANGE", "a:l", 0, -1)).isEqualTo(List.of("x", "y", "z"));
        assertThat(ok("LINDEX", "a:l", -1)).isEqualTo("z");
        assertThat(ok("LPOP", "a:l")).isEqualTo("x");

        assertThat(ok("SADD", "a:s", "m1", "m2", "m1")).isEqualTo(2L);
        assertThat(ok("SCARD", "a:s")).isEqualTo(2L);
        assertThat(ok("SISMEMBER", "a:s", "m2")).isEqualTo(true);
        assertThat(ok("SREM", "a:s", "m1", "m2")).isEqualTo(2L);

        assertThat(ok("ZADD", "a:z", 1.0, "one", 2.0, "two")).isEqualTo(2L);
        assertThat(ok("ZCARD", "a:z")).isEqualTo(2L);
        assertThat(ok("ZSCORE", "a:z", "two")).isEqualTo(2.0);
        assertThat(ok("ZRANK", "a:z", "two")).isEqualTo(1L);
        assertThat(ok("ZRANGE", "a:z", 0, -1)).isEqualTo(List.of("one", "two"));
        assertThat(ok("ZREM", "a:z", "one")).isEqualTo(1L);
    }

    @Test
    @DisplayName("keyspace/原子命令：SET/GET/TYPE/PTTL/INCR/CAS/MGET/MSET")
    void keyspaceCommands() {
        assertThat(ok("SET", "a:k", "v", 60_000L)).isEqualTo("OK");
        assertThat(ok("GET", "a:k")).isEqualTo("v");
        assertThat(ok("EXISTS", "a:k")).isEqualTo(true);
        assertThat(ok("TYPE", "a:k")).isEqualTo("string");
        assertThat(((Number) ok("PTTL", "a:k")).longValue()).isBetween(1L, 60_000L);
        assertThat(ok("PERSIST", "a:k")).isEqualTo(true);
        assertThat(ok("PTTL", "a:k")).isEqualTo(-1L);

        assertThat(ok("INCR", "a:counter")).isEqualTo(1L);
        assertThat(ok("INCRBY", "a:counter", 4)).isEqualTo(5L);
        assertThat(ok("CAS", "a:counter", 5L, 9L)).isEqualTo(true);
        assertThat(ok("CAS", "a:counter", 5L, 10L)).isEqualTo(false);
        assertThat(ok("GET", "a:counter")).isEqualTo(9L);

        assertThat(ok("MSET", "a:m1", "v1", "a:m2", "v2")).isEqualTo(2L);
        assertThat(ok("MGET", "a:m1", "a:m2", "a:missing"))
                .isEqualTo(java.util.Arrays.asList("v1", "v2", null));

        CacheCommandResponse scan = exec("SCAN", "", "MATCH", "a:m", "COUNT", 10);
        assertThat(scan.ok()).isTrue();
        assertThat((List<?>) ((Map<?, ?>) scan.result()).get("keys")).hasSize(2);
        assertThat(ok("STATS")).isInstanceOf(Map.class);
    }

    @Test
    @DisplayName("MSET 跨分区返回 CROSSSLOT；未知命令返回 UNKNOWN_COMMAND")
    void commandErrors() {
        CacheCommandResponse crossSlot = exec("MSET", "a:k1", "v1", "b:k2", "v2");
        assertThat(crossSlot.ok()).isFalse();
        assertThat(crossSlot.code()).isEqualTo(CacheCommandException.CROSSSLOT);
        assertThat(capability.get("a:k1")).isNull();
        assertThat(capability.get("b:k2")).isNull();

        CacheCommandResponse unknown = exec("NOPE", "a:k");
        assertThat(unknown.ok()).isFalse();
        assertThat(unknown.code()).isEqualTo(CacheCommandException.UNKNOWN_COMMAND);
    }

    @Test
    @DisplayName("Stream 命令：XADD/XRANGE/XGROUP/XREADGROUP/XACK/XINFO")
    @SuppressWarnings("unchecked")
    void streamCommands() {
        Object id = ok("XADD", "a:stream", "*", "f", "v");
        assertThat(id).isInstanceOf(String.class);
        assertThat(ok("XLEN", "a:stream")).isEqualTo(1L);

        List<Object> range = (List<Object>) ok("XRANGE", "a:stream", "-", "+");
        assertThat(range).hasSize(1);
        String entryId = String.valueOf(((Map<String, Object>) range.get(0)).get("id"));

        assertThat(ok("XGROUP", "CREATE", "a:stream", "g", "0", "MKSTREAM")).isEqualTo(true);
        assertThat((List<?>) ok("XREADGROUP", "GROUP", "g", "c", "COUNT", 10,
                "STREAMS", "a:stream", ">")).hasSize(1);
        assertThat((Map<String, Object>) ok("XPENDING", "a:stream", "g"))
                .containsEntry("total", 1L);
        assertThat(ok("XACK", "a:stream", "g", entryId)).isEqualTo(1L);

        Map<String, Object> info = (Map<String, Object>) ok("XINFO", "STREAM", "a:stream");
        assertThat(info).containsEntry("length", 1L);
        assertThat(ok("TYPE", "a:stream")).isEqualTo("stream");
        assertThat(ok("DEL", "a:stream")).isEqualTo(true);
        assertThat(ok("TYPE", "a:stream")).isEqualTo("none");
    }
}
