package com.ww.admin.controller;


import com.ww.cache.Cache;
import com.ww.cache.SimpleCache;
import com.ww.cache.StreamId;
import com.ww.cache.XAddOptions;
import com.ww.admin.common.JsonResult;
import com.ww.common.query.Pageable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @author Jw
 * @version 1.0
 * @datetime 2026/6/25 17:07
 */
@Controller
@RequestMapping("/cache")
public class CacheController {

    private static final Logger log = LoggerFactory.getLogger(CacheController.class);

    @Autowired
    private Cache<String, Object> cache;

    /**
     * 本地 SimpleCache：供 HTTP Stream API 直接调用。
     * <p>集群模式下 Stream 走 TCP（HRW 路由），HTTP 仅作单机调试用途。
     */
    @Autowired
    private SimpleCache<String, Object> localSimpleCache;

    @PostMapping("/put")
    @ResponseBody
    public JsonResult putVal(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        cache.put(key, map.get("value"));
        return JsonResult.success(true);
    }

    @PostMapping("/get")
    @ResponseBody
    public JsonResult getVal(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        try {
            return JsonResult.success(cache.get(key));
        } catch (Exception e) {
            log.error("缓存读取失败: key={}", key, e);
            return JsonResult.warn("缓存读取失败: " + e.getMessage());
        }
    }

    @PostMapping("/putExpired")
    @ResponseBody
    public JsonResult putExpired(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        Object ttlObj = map.get("ttlMillis");
        if (ttlObj == null) return JsonResult.warn("缺少参数 ttlMillis");
        try {
            cache.put(key, map.get("value"), Long.parseLong(ttlObj.toString()));
            return JsonResult.success(true);
        } catch (NumberFormatException e) {
            return JsonResult.warn("ttlMillis 格式错误");
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public JsonResult remove(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        cache.remove(key);
        return JsonResult.success(true);
    }

    @PostMapping("/keys")
    @ResponseBody
    public JsonResult keys() {
        return JsonResult.success(cache.keys());
    }

    /** 从请求参数中提取 key，缺失或类型错误返回 null */
    private static String requireKey(Map<String, Object> map) {
        if (map == null) return null;
        Object k = map.get("key");
        return k instanceof String s ? s : (k != null ? k.toString() : null);
    }

    // ==================== Hash 操作 ====================

    @PostMapping("/hset")
    @ResponseBody
    public JsonResult hset(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        Object field = map.get("field");
        if (field == null) return JsonResult.warn("缺少参数 field");
        cache.hset(key, field.toString(), map.get("value"));
        return JsonResult.success(true);
    }

    @PostMapping("/hget")
    @ResponseBody
    public JsonResult hget(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        Object field = map.get("field");
        if (field == null) return JsonResult.warn("缺少参数 field");
        return JsonResult.success(cache.hget(key, field.toString()));
    }

    @PostMapping("/hmget")
    @ResponseBody
    @SuppressWarnings("unchecked")
    public JsonResult hmget(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        List<String> fields = (List<String>) map.get("fields");
        if (fields == null || fields.isEmpty()) return JsonResult.warn("缺少参数 fields");
        return JsonResult.success(cache.hmget(key, fields.toArray(new String[0])));
    }

    @PostMapping("/hgetAll")
    @ResponseBody
    public JsonResult hgetAll(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        return JsonResult.success(cache.hgetAll(key));
    }

    // ==================== List 操作 ====================

    @PostMapping("/lpush")
    @ResponseBody
    @SuppressWarnings("unchecked")
    public JsonResult lpush(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        List<Object> values = (List<Object>) map.get("values");
        if (values == null || values.isEmpty()) return JsonResult.warn("缺少参数 values");
        cache.lpush(key, values.toArray(new Object[0]));
        return JsonResult.success(true);
    }

    @PostMapping("/rpush")
    @ResponseBody
    @SuppressWarnings("unchecked")
    public JsonResult rpush(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        List<Object> values = (List<Object>) map.get("values");
        if (values == null || values.isEmpty()) return JsonResult.warn("缺少参数 values");
        cache.rpush(key, values.toArray(new Object[0]));
        return JsonResult.success(true);
    }

    @PostMapping("/lpop")
    @ResponseBody
    public JsonResult lpop(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        return JsonResult.success(cache.lpop(key));
    }

    @PostMapping("/rpop")
    @ResponseBody
    public JsonResult rpop(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        return JsonResult.success(cache.rpop(key));
    }

    // ==================== Set 操作 ====================

    @PostMapping("/sadd")
    @ResponseBody
    @SuppressWarnings("unchecked")
    public JsonResult sadd(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        List<Object> members = (List<Object>) map.get("members");
        if (members == null || members.isEmpty()) return JsonResult.warn("缺少参数 members");
        cache.sadd(key, members.toArray(new Object[0]));
        return JsonResult.success(true);
    }

    @PostMapping("/smembers")
    @ResponseBody
    public JsonResult smembers(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        return JsonResult.success(cache.smembers(key));
    }

    @PostMapping("/srem")
    @ResponseBody
    @SuppressWarnings("unchecked")
    public JsonResult srem(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        List<Object> members = (List<Object>) map.get("members");
        if (members == null || members.isEmpty()) return JsonResult.warn("缺少参数 members");
        cache.srem(key, members.toArray(new Object[0]));
        return JsonResult.success(true);
    }

    // ==================== Sorted Set 操作 ====================

    @PostMapping("/zadd")
    @ResponseBody
    public JsonResult zadd(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        Object scoreObj = map.get("score");
        if (!(scoreObj instanceof Number)) return JsonResult.warn("缺少参数 score");
        cache.zadd(key, ((Number) scoreObj).doubleValue(), map.get("member"));
        return JsonResult.success(true);
    }

    @PostMapping("/zrange")
    @ResponseBody
    public JsonResult zrange(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        Object startObj = map.get("start"), stopObj = map.get("stop");
        if (!(startObj instanceof Number) || !(stopObj instanceof Number))
            return JsonResult.warn("缺少参数 start/stop");
        return JsonResult.success(cache.zrange(key, ((Number) startObj).longValue(), ((Number) stopObj).longValue()));
    }

    @PostMapping("/zrangeByScore")
    @ResponseBody
    public JsonResult zrangeByScore(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        Object minObj = map.get("min"), maxObj = map.get("max");
        if (!(minObj instanceof Number) || !(maxObj instanceof Number))
            return JsonResult.warn("缺少参数 min/max");
        return JsonResult.success(cache.zrangeByScore(key, ((Number) minObj).doubleValue(), ((Number) maxObj).doubleValue()));
    }

    // ==================== 事务操作 ====================

    @PostMapping("/multi")
    @ResponseBody
    public JsonResult multi() {
        cache.multi();
        return JsonResult.success("OK");
    }

    @PostMapping("/exec")
    @ResponseBody
    public JsonResult exec() {
        List<Object> results = cache.exec();
        if (results == null) {
            return JsonResult.warn("WATCH conflict, transaction aborted");
        }
        return JsonResult.success(results);
    }

    @PostMapping("/discard")
    @ResponseBody
    public JsonResult discard() {
        cache.discard();
        return JsonResult.success("OK");
    }

    @PostMapping("/watch")
    @ResponseBody
    @SuppressWarnings("unchecked")
    public JsonResult watch(@RequestBody Map<String, Object> map) {
        List<String> keys = (List<String>) map.get("keys");
        cache.watch(keys.toArray(new String[0]));
        return JsonResult.success("OK");
    }

    // ==================== Stream 操作（HTTP 调试接口） ====================
    // 注意：阻塞 XREAD/XREADGROUP 不在 HTTP 暴露（长连接不适合 HTTP 请求-响应模型），请走 TCP。
    // 集群模式下 Stream 走 TCP HRW 路由；HTTP 仅操作本地节点，用于开发调试。

    @PostMapping("/stream/xadd")
    @ResponseBody
    @SuppressWarnings("unchecked")
    public JsonResult streamXAdd(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        Object fieldsObj = map.get("fields");
        if (!(fieldsObj instanceof Map<?, ?>)) return JsonResult.warn("缺少参数 fields（需为 KV 对象）");
        Map<String, String> fields = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : ((Map<?, ?>) fieldsObj).entrySet()) {
            fields.put(String.valueOf(e.getKey()), e.getValue() == null ? "" : String.valueOf(e.getValue()));
        }
        XAddOptions.Builder opts = XAddOptions.builder();
        Object idObj = map.get("id");
        StreamId idReq = (idObj instanceof String s && !s.isEmpty()) ? StreamId.parse(s) : StreamId.AUTO;
        Object maxLenObj = map.get("maxLen");
        if (maxLenObj instanceof Number n) opts.maxLen(n.longValue());
        Object nomkstreamObj = map.get("nomkstream");
        if (Boolean.TRUE.equals(nomkstreamObj)) opts.nomkstream(true);
        Object approximateObj = map.get("approximate");
        if (Boolean.TRUE.equals(approximateObj)) opts.approximate(true);
        try {
            StreamId result = localSimpleCache.xadd(key, idReq, fields, opts.build());
            return JsonResult.success(result == null ? null : result.toString());
        } catch (Exception e) {
            log.error("XADD 失败: key={}", key, e);
            return JsonResult.warn("XADD 失败: " + e.getMessage());
        }
    }

    @PostMapping("/stream/xlen")
    @ResponseBody
    public JsonResult streamXLen(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        return JsonResult.success(localSimpleCache.getStreamStore().xlen(key));
    }

    @PostMapping("/stream/xrange")
    @ResponseBody
    public JsonResult streamXRange(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        String min = map.getOrDefault("min", "-").toString();
        String max = map.getOrDefault("max", "+").toString();
        long count = map.get("count") instanceof Number n ? n.longValue() : -1L;
        return JsonResult.success(localSimpleCache.getStreamStore()
                .xrange(key, StreamId.parse(min), StreamId.parse(max), count));
    }

    @PostMapping("/stream/xrevrange")
    @ResponseBody
    public JsonResult streamXRevRange(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        String min = map.getOrDefault("min", "-").toString();
        String max = map.getOrDefault("max", "+").toString();
        long count = map.get("count") instanceof Number n ? n.longValue() : -1L;
        return JsonResult.success(localSimpleCache.getStreamStore()
                .xrevrange(key, StreamId.parse(max), StreamId.parse(min), count));
    }

    @PostMapping("/stream/xdel")
    @ResponseBody
    @SuppressWarnings("unchecked")
    public JsonResult streamXDel(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        List<String> ids = (List<String>) map.get("ids");
        if (ids == null || ids.isEmpty()) return JsonResult.warn("缺少参数 ids");
        StreamId[] streamIds = ids.stream().map(StreamId::parse).toArray(StreamId[]::new);
        return JsonResult.success(localSimpleCache.xdel(key, streamIds));
    }

    @PostMapping("/stream/xtrim")
    @ResponseBody
    public JsonResult streamXTrim(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        Object maxLenObj = map.get("maxLen");
        if (!(maxLenObj instanceof Number)) return JsonResult.warn("缺少参数 maxLen");
        boolean approximate = Boolean.TRUE.equals(map.get("approximate"));
        return JsonResult.success(localSimpleCache.xtrim(key, ((Number) maxLenObj).longValue(), approximate));
    }

    @PostMapping("/stream/xinfo")
    @ResponseBody
    public JsonResult streamXInfo(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        return JsonResult.success(localSimpleCache.getStreamStore().xinfo(key));
    }

    @PostMapping("/stream/xinfoGroups")
    @ResponseBody
    public JsonResult streamXInfoGroups(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        return JsonResult.success(localSimpleCache.getStreamStore().xinfoGroups(key));
    }

    @PostMapping("/stream/xinfoConsumers")
    @ResponseBody
    public JsonResult streamXInfoConsumers(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        Object group = map.get("group");
        if (group == null) return JsonResult.warn("缺少参数 group");
        return JsonResult.success(localSimpleCache.getStreamStore().xinfoConsumers(key, group.toString()));
    }

    @PostMapping("/stream/xgroupCreate")
    @ResponseBody
    public JsonResult streamXGroupCreate(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        Object group = map.get("group");
        if (group == null) return JsonResult.warn("缺少参数 group");
        String start = map.getOrDefault("start", "0").toString();
        boolean mkstream = Boolean.TRUE.equals(map.get("mkstream"));
        boolean created = localSimpleCache.xgroupCreate(key, group.toString(), StreamId.parse(start), mkstream);
        return JsonResult.success(created);
    }

    @PostMapping("/stream/xgroupDestroy")
    @ResponseBody
    public JsonResult streamXGroupDestroy(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        Object group = map.get("group");
        if (group == null) return JsonResult.warn("缺少参数 group");
        // 走 SimpleCache 包装路径以记录 AOF（保证重启后组删除语义保留）
        return JsonResult.success(localSimpleCache.xgroupDestroy(key, group.toString()));
    }

    @PostMapping("/stream/xack")
    @ResponseBody
    @SuppressWarnings("unchecked")
    public JsonResult streamXAck(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        Object group = map.get("group");
        if (group == null) return JsonResult.warn("缺少参数 group");
        List<String> ids = (List<String>) map.get("ids");
        if (ids == null || ids.isEmpty()) return JsonResult.warn("缺少参数 ids");
        StreamId[] streamIds = ids.stream().map(StreamId::parse).toArray(StreamId[]::new);
        return JsonResult.success(localSimpleCache.xack(key, group.toString(), streamIds));
    }

    @PostMapping("/stream/xpending")
    @ResponseBody
    public JsonResult streamXPending(@RequestBody Map<String, Object> map) {
        String key = requireKey(map);
        if (key == null) return JsonResult.warn("缺少参数 key");
        Object group = map.get("group");
        if (group == null) return JsonResult.warn("缺少参数 group");
        String start = map.getOrDefault("start", "-").toString();
        String end = map.getOrDefault("end", "+").toString();
        long count = map.get("count") instanceof Number n ? n.longValue() : -1L;
        String consumer = map.get("consumer") instanceof String s ? s : null;
        long minIdle = map.get("minIdleMs") instanceof Number n2 ? n2.longValue() : 0L;
        return JsonResult.success(localSimpleCache.getStreamStore()
                .xpending(key, group.toString(), StreamId.parse(start), StreamId.parse(end), count, consumer, minIdle));
    }

}
