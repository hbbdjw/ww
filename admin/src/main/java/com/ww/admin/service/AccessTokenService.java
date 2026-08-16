package com.ww.admin.service;

import com.ww.admin.entity.AccessToken;
import com.ww.admin.entity.QAccessToken;
import com.ww.admin.entity.SysUser;
import com.ww.admin.security.TokenSnapshot;
import com.ww.cache.Cache;
import com.ww.common.dao.BaseDaoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Date;
import java.util.List;

/**
 * 访问令牌服务：签发、校验、吊销。
 * <p>
 * 令牌为 32 字节随机数的 Base64URL 表示；数据库仅存其 SHA-256 哈希，
 * 原始令牌只在签发时返回客户端一次。
 * <p>
 * 集群模式：令牌校验结果（{@link TokenSnapshot}）缓存到分布式缓存，
 * 避免每次 API 请求都查库，并实现集群内令牌验证结果共享。
 * 吊销时立即移除缓存条目，满足 read-after-write 一致性。
 */
@Service
public class AccessTokenService {

    private static final Logger log = LoggerFactory.getLogger(AccessTokenService.class);

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int TOKEN_BYTES = 32;
    private static final String CACHE_PREFIX = "shiro:token:";

    private final BaseDaoImpl baseDao;
    private final Cache<String, Object> distributedCache;

    public AccessTokenService(BaseDaoImpl baseDao, Cache<String, Object> distributedCache) {
        this.baseDao = baseDao;
        this.distributedCache = distributedCache;
    }

    /**
     * 为用户签发新令牌。
     *
     * @param user      用户
     * @param ttlMillis 有效期（毫秒）
     * @param clientIp  客户端 IP（审计用）
     * @return 原始令牌字符串（仅此一次返回）
     */
    public String issue(SysUser user, long ttlMillis, String clientIp) {
        byte[] bytes = new byte[TOKEN_BYTES];
        RANDOM.nextBytes(bytes);
        String rawToken = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        String tokenHash = sha256Hex(rawToken);

        AccessToken at = new AccessToken();
        at.setTokenHash(tokenHash);
        at.setUser(user);
        at.setExpiresAt(new Date(System.currentTimeMillis() + ttlMillis));
        at.setClientIp(clientIp);
        at.setRevoked(Boolean.FALSE);
        baseDao.persist(at);

        // 预热缓存：签发时直接写入快照
        TokenSnapshot snapshot = new TokenSnapshot(
                tokenHash,
                user.getId(),
                user.getUsername(),
                user.getStatus(),
                System.currentTimeMillis() + ttlMillis,
                false
        );
        distributedCache.put(CACHE_PREFIX + tokenHash, snapshot, ttlMillis);
        return rawToken;
    }

    /**
     * 根据原始令牌查找有效的访问令牌记录（直接查库，不走缓存）。
     *
     * @param rawToken 原始令牌
     * @return 令牌记录；不存在或已失效返回 null
     */
    public AccessToken findValid(String rawToken) {
        if (rawToken == null || rawToken.isEmpty()) {
            return null;
        }
        AccessToken at = baseDao.queryFactory()
                .selectFrom(QAccessToken.accessToken)
                .where(QAccessToken.accessToken.tokenHash.eq(sha256Hex(rawToken)))
                .fetchOne();
        if (at == null || at.isInvalid()) {
            return null;
        }
        return at;
    }

    /**
     * 根据原始令牌获取令牌快照（优先走分布式缓存，未命中查库并回填）。
     * <p>
     * 缓存键：{@code shiro:token:<tokenHash>}，TTL = 令牌剩余有效期。
     * 缓存命中时直接返回快照，避免 DB 查询；集群内任意节点缓存的结果可被其他节点复用。
     *
     * @param rawToken 原始令牌
     * @return 令牌快照；不存在或已失效返回 null
     */
    public TokenSnapshot findValidSnapshot(String rawToken) {
        if (rawToken == null || rawToken.isEmpty()) {
            return null;
        }
        String tokenHash = sha256Hex(rawToken);
        String cacheKey = CACHE_PREFIX + tokenHash;

        // 1. 优先读缓存
        try {
            Object cached = distributedCache.get(cacheKey);
            if (cached instanceof TokenSnapshot snap) {
                // 二次校验：缓存中的快照可能已过期或吊销（TTL 未到期但状态已变更）
                if (isSnapshotValid(snap)) {
                    return snap;
                }
                // 快照已失效，移除缓存条目
                distributedCache.remove(cacheKey);
                return null;
            }
        } catch (Exception e) {
            log.debug("读取令牌缓存失败，降级查库: hash={}, err={}", tokenHash, e.getMessage());
        }

        // 2. 缓存未命中 → 查库
        AccessToken at = baseDao.queryFactory()
                .selectFrom(QAccessToken.accessToken)
                .where(QAccessToken.accessToken.tokenHash.eq(tokenHash))
                .fetchOne();
        if (at == null || at.isInvalid()) {
            return null;
        }

        // 3. 回填缓存（TTL = 令牌剩余有效期）
        SysUser user = at.getUser();
        long remainingTtl = at.getExpiresAt().getTime() - System.currentTimeMillis();
        if (remainingTtl > 0) {
            TokenSnapshot snapshot = new TokenSnapshot(
                    tokenHash,
                    user != null ? user.getId() : null,
                    user != null ? user.getUsername() : null,
                    user != null ? user.getStatus() : null,
                    at.getExpiresAt().getTime(),
                    Boolean.TRUE.equals(at.getRevoked())
            );
            try {
                distributedCache.put(cacheKey, snapshot, remainingTtl);
            } catch (Exception e) {
                log.debug("回填令牌缓存失败: hash={}, err={}", tokenHash, e.getMessage());
            }
        }
        return new TokenSnapshot(
                tokenHash,
                user != null ? user.getId() : null,
                user != null ? user.getUsername() : null,
                user != null ? user.getStatus() : null,
                at.getExpiresAt().getTime(),
                Boolean.TRUE.equals(at.getRevoked())
        );
    }

    private boolean isSnapshotValid(TokenSnapshot snap) {
        if (snap.revoked()) {
            return false;
        }
        return snap.expiresAtMillis() > System.currentTimeMillis();
    }

    /**
     * 吊销指定原始令牌。同时移除缓存条目。
     */
    public boolean revoke(String rawToken) {
        if (rawToken == null || rawToken.isEmpty()) {
            return false;
        }
        String tokenHash = sha256Hex(rawToken);
        AccessToken at = baseDao.queryFactory()
                .selectFrom(QAccessToken.accessToken)
                .where(QAccessToken.accessToken.tokenHash.eq(tokenHash))
                .fetchOne();
        if (at == null || at.isInvalid()) {
            return false;
        }
        at.setRevoked(Boolean.TRUE);
        baseDao.merge(at);
        // 立即移除缓存，满足 read-after-write 一致性
        distributedCache.remove(CACHE_PREFIX + tokenHash);
        return true;
    }

    /**
     * 吊销某用户全部令牌（修改密码 / 强制下线时使用）。
     * 同时批量移除该用户所有令牌的缓存条目。
     */
    public int revokeAllByUser(Long userId) {
        // 先查出该用户所有未吊销令牌的哈希，用于清缓存
        List<String> tokenHashes = baseDao.queryFactory()
                .select(QAccessToken.accessToken.tokenHash)
                .from(QAccessToken.accessToken)
                .where(QAccessToken.accessToken.user.id.eq(userId)
                        .and(QAccessToken.accessToken.revoked.eq(false)))
                .fetch();
        // 更新数据库
        int updated = baseDao.updateBySQL(
                "UPDATE sys_access_token SET revoked = true WHERE user_id = :uid AND revoked = false",
                java.util.Map.of("uid", userId));
        // 批量移除缓存
        for (String hash : tokenHashes) {
            distributedCache.remove(CACHE_PREFIX + hash);
        }
        log.info("吊销用户 {} 全部令牌: {} 条, 已清缓存 {} 条", userId, updated, tokenHashes.size());
        return updated;
    }

    /**
     * 清理已过期或已吊销的令牌记录。
     */
    public int purgeInvalid() {
        return baseDao.updateBySQL(
                "DELETE FROM sys_access_token WHERE revoked = true OR expires_at < now()",
                java.util.Map.of());
    }

    static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16))
                  .append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

}
