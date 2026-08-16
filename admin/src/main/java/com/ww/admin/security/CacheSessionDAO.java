package com.ww.admin.security;

import com.ww.cache.Cache;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.UnknownSessionException;
import org.apache.shiro.session.mgt.eis.AbstractSessionDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

/**
 * 基于分布式缓存的 Shiro SessionDAO，实现集群会话共享。
 * <p>
 * 会话以 {@code shiro:session:<sessionId>} 为键存入分布式缓存，TTL = session.getTimeout()。
 * 任意节点创建的会话可通过同一 sessionId 在其他节点读取，实现 Session 集群共享。
 * <p>
 * Session 对象通过 Codec 的 TYPE_OBJECT（Java 序列化）存储，{@link org.apache.shiro.session.mgt.SimpleSession}
 * 是 Serializable，满足编解码要求。
 * <p>
 * {@link #getActiveSessions()} 返回空集合：不枚举活动会话，依赖缓存 TTL 自动过期清理，
 * 与 {@code DefaultWebSessionManager.setDeleteInvalidSessions(true)} 配合由缓存 TTL 保障过期语义。
 */
public class CacheSessionDAO extends AbstractSessionDAO {

    private static final Logger log = LoggerFactory.getLogger(CacheSessionDAO.class);
    private static final String PREFIX = "shiro:session:";

    private final Cache<String, Object> cache;

    public CacheSessionDAO(Cache<String, Object> cache) {
        this.cache = cache;
    }

    private String key(Serializable sessionId) {
        return PREFIX + sessionId;
    }

    @Override
    protected Serializable doCreate(Session session) {
        Serializable sessionId = generateSessionId(session);
        assignSessionId(session, sessionId);
        store(session);
        log.debug("创建会话: id={}", sessionId);
        return sessionId;
    }

    @Override
    protected Session doReadSession(Serializable sessionId) {
        if (sessionId == null) {
            return null;
        }
        try {
            Object obj = cache.get(key(sessionId));
            if (obj instanceof Session s) {
                return s;
            }
            return null;
        } catch (Exception e) {
            log.warn("读取会话失败: id={}, err={}", sessionId, e.getMessage());
            return null;
        }
    }

    @Override
    public void update(Session session) throws UnknownSessionException {
        if (session == null || session.getId() == null) {
            return;
        }
        // 会话已停止或过期 → 删除
        if (session.getStartTimestamp() != null) {
            long timeout = session.getTimeout();
            long elapsed = System.currentTimeMillis() - session.getStartTimestamp().getTime();
            if (timeout > 0 && elapsed > timeout) {
                delete(session);
                return;
            }
        }
        store(session);
    }

    private void store(Session session) {
        long ttl = session.getTimeout();
        if (ttl <= 0) {
            ttl = 30 * 60 * 1000L;
        }
        cache.put(key(session.getId()), session, ttl);
    }

    @Override
    public void delete(Session session) {
        if (session == null || session.getId() == null) {
            return;
        }
        cache.remove(key(session.getId()));
        log.debug("删除会话: id={}", session.getId());
    }

    @Override
    public Collection<Session> getActiveSessions() {
        // 不枚举活动会话；依赖缓存 TTL 自动过期，避免跨节点全量扫描
        return Collections.emptyList();
    }
}
