package com.ww.runtime.data.cache.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link AofFormat} 记录编码格式锚点测试（R1 抽取，行为不变基线）。
 *
 * <p>锁定记录布局：{@code marker + opCode + keyLen + key + argCount + [argLen + arg]* + crc32}，
 * 防止后续格式演进意外破坏存量 AOF 兼容。
 */
@DisplayName("AofFormat 记录编码")
class AofFormatTest {

    /** 按既定布局解析一条记录，返回 [op, key, args...]（不含 CRC）。 */
    private static ParsedRecord parse(byte[] record) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(record));
        byte marker = in.readByte();
        byte op = in.readByte();
        int keyLen = in.readUnsignedShort();
        byte[] keyBytes = new byte[keyLen];
        in.readFully(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        int argCount = in.readUnsignedShort();
        Object[] args = new Object[argCount];
        for (int i = 0; i < argCount; i++) {
            int argLen = in.readInt();
            byte[] argBytes = new byte[argLen];
            in.readFully(argBytes);
            args[i] = Codec.decodeValue(argBytes);
        }
        int storedCrc = in.readInt();
        assertThat(in.available()).isZero();
        return new ParsedRecord(marker, op, key, args, storedCrc);
    }

    private record ParsedRecord(byte marker, byte op, String key, Object[] args, int storedCrc) {
    }

    @Test
    @DisplayName("记录布局：marker + op + key + args + 尾部 4 字节大端 CRC")
    void recordLayoutAndCrc() throws IOException {
        byte[] record = AofFormat.encodeCommand(AofFormat.OP_PUT, "user:1", "hello");

        ParsedRecord parsed = parse(record);
        assertThat(parsed.marker()).isEqualTo(AofFormat.CMD_MARKER);
        assertThat(parsed.op()).isEqualTo(AofFormat.OP_PUT);
        assertThat(parsed.key()).isEqualTo("user:1");
        assertThat(parsed.args()).containsExactly("hello");

        CRC32 crc = new CRC32();
        crc.update(record, 0, record.length - 4);
        int expected = (int) crc.getValue();
        assertThat(parsed.storedCrc()).isEqualTo(expected);
    }

    @Test
    @DisplayName("多参数类型往返：String/Long/Double/Boolean/List/Map")
    void multiArgTypeRoundTrip() throws IOException {
        List<Object> listArg = new ArrayList<>(List.of("a", "b"));
        Map<String, Object> mapArg = new LinkedHashMap<>();
        mapArg.put("f1", "v1");
        mapArg.put("f2", 2);

        byte[] record = AofFormat.encodeCommand(AofFormat.OP_HSET, "k",
                "field", 42L, 3.5d, true, listArg, mapArg);

        ParsedRecord parsed = parse(record);
        assertThat(parsed.op()).isEqualTo(AofFormat.OP_HSET);
        assertThat(parsed.key()).isEqualTo("k");
        assertThat(parsed.args()).hasSize(6);
        assertThat(parsed.args()[0]).isEqualTo("field");
        assertThat(parsed.args()[1]).isEqualTo(42L);
        assertThat(parsed.args()[2]).isEqualTo(3.5d);
        assertThat(parsed.args()[3]).isEqualTo(true);
        assertThat(parsed.args()[4]).isEqualTo(listArg);
        assertThat(parsed.args()[5]).isEqualTo(mapArg);
    }

    @Test
    @DisplayName("无参命令编码为空参数列表")
    void noArgCommand() throws IOException {
        byte[] record = AofFormat.encodeCommand(AofFormat.OP_REMOVE, "k");

        ParsedRecord parsed = parse(record);
        assertThat(parsed.op()).isEqualTo(AofFormat.OP_REMOVE);
        assertThat(parsed.args()).isEmpty();
    }

    @Test
    @DisplayName("UTF-8 key 按字节长度编码并完整往返")
    void utf8KeyRoundTrip() throws IOException {
        String key = "缓存:用户:1001";
        byte[] record = AofFormat.encodeCommand(AofFormat.OP_PUT, key, "v");

        ParsedRecord parsed = parse(record);
        assertThat(parsed.key()).isEqualTo(key);
    }

    @Test
    @DisplayName("Reader 往返：编码记录经解析完整还原，CRC 校验生效")
    void readerRoundTripAndCrcEnforced() throws IOException {
        byte[] record = AofFormat.encodeCommand(AofFormat.OP_HSET, "h1", "field", 42L);

        AofFormat.Reader reader = new AofFormat.Reader(new ByteArrayInputStream(record));
        AofFormat.Record parsed = reader.readRecord();
        assertThat(parsed.opCode()).isEqualTo(AofFormat.OP_HSET);
        assertThat(parsed.key()).isEqualTo("h1");
        assertThat(parsed.args()).containsExactly("field", 42L);

        // 篡改值区一字节：CRC 失配必须响亮失败
        byte[] corrupted = record.clone();
        corrupted[corrupted.length - 5] ^= (byte) 0xFF;
        AofFormat.Reader badReader = new AofFormat.Reader(new ByteArrayInputStream(corrupted));
        org.assertj.core.api.Assertions.assertThatThrownBy(badReader::readRecord)
                .isInstanceOf(AofFormat.AofCrcMismatchException.class);
    }

    @Test
    @DisplayName("C34：超过 64KB 的 key 编码响亮失败，不得静默截断")
    void oversizedKeyRejectedAtEncode() {
        String longKey = "k".repeat(70_000);
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> AofFormat.encodeCommand(AofFormat.OP_PUT, longKey, "v"))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("拒绝写入");
    }

    @Test
    @DisplayName("opCode 与 opName 全量互逆，未知操作分别返回 -1 与 UNKNOWN")
    void opCodeNameMapping() {
        String[] ops = {
                "PUT", "PUT_EXPIRED", "REMOVE", "CLEAR", "HSET", "LPUSH", "RPUSH",
                "LPOP", "RPOP", "SADD", "SREM", "ZADD", "XADD", "XDEL", "XTRIM",
                "XGROUP_CREATE", "XACK", "XGROUP_DESTROY", "XGROUP_CREATECONSUMER",
                "XGROUP_DELCONSUMER", "XCLAIM", "XDELIVER", "XPEL_ENTRY",
        };
        for (String op : ops) {
            byte code = AofFormat.opCode(op);
            assertThat(code).as("opCode of %s", op).isNotEqualTo((byte) -1);
            assertThat(AofFormat.opName(code)).isEqualTo(op);
        }
        assertThat(AofFormat.opCode("NO_SUCH_OP")).isEqualTo((byte) -1);
        assertThat(AofFormat.opName((byte) 99)).isEqualTo("UNKNOWN(99)");
    }

    @Test
    @DisplayName("ZADD 记录：score + member 双参数往返")
    void zaddRecordRoundTrip() throws IOException {
        byte[] record = AofFormat.encodeCommand(AofFormat.OP_ZADD, "rank", 9.5d, "member-a");

        ParsedRecord parsed = parse(record);
        assertThat(parsed.op()).isEqualTo(AofFormat.OP_ZADD);
        assertThat(parsed.args()).containsExactly(9.5d, "member-a");
    }
}
