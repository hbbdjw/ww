package com.ww.admin.entity;

import com.ww.common.entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

/**
 * 访问令牌（Token 认证方式）。
 * <p>
 * 仅存储令牌的 SHA-256 哈希（token_hash），原始令牌只在签发时返回客户端一次。
 * 支持过期与吊销，满足"短期有效、可审计、可撤销"的要求。
 */
@Getter
@Setter
@Entity
@Table(name = "sys_access_token")
public class AccessToken extends BaseEntity {

    /**
     * 令牌哈希（SHA-256(rawToken) 十六进制）
     */
    @Column(name = "token_hash", nullable = false, unique = true, length = 64)
    private String tokenHash;

    /**
     * 所属用户
     */
    @ManyToOne(fetch = FetchType.EAGER, optional = false)
    @JoinColumn(name = "user_id", nullable = false)
    private SysUser user;

    /**
     * 过期时间
     */
    @Column(name = "expires_at", nullable = false)
    private Date expiresAt;

    /**
     * 是否已吊销
     */
    @Column(name = "revoked", nullable = false)
    private Boolean revoked = Boolean.FALSE;

    /**
     * 客户端 IP（签发时记录，便于审计）
     */
    @Column(name = "client_ip")
    private String clientIp;

    /**
     * 是否已过期或已吊销
     */
    public boolean isInvalid() {
        return Boolean.TRUE.equals(revoked) || (expiresAt != null && expiresAt.before(new Date()));
    }

}
