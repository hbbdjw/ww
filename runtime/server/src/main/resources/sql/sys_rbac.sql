-- RBAC 权限体系 DDL（PostgreSQL）
-- 由 com.ww.runtime.server.security.RbacSchema 幂等执行（CREATE ... IF NOT EXISTS）。

-- 部门表（树形，parent_id=0 表示根部门）
CREATE TABLE IF NOT EXISTS sys_dept (
    dept_id    BIGSERIAL    PRIMARY KEY,
    parent_id  BIGINT       NOT NULL DEFAULT 0,
    dept_name  VARCHAR(64)  NOT NULL,
    order_num  INT          NOT NULL DEFAULT 0,
    leader     VARCHAR(32),
    phone      VARCHAR(16),
    status     SMALLINT     NOT NULL DEFAULT 1,
    created_at BIGINT       NOT NULL,
    updated_at BIGINT       NOT NULL
);

-- 用户表
CREATE TABLE IF NOT EXISTS sys_user (
    user_id    BIGSERIAL    PRIMARY KEY,
    dept_id    BIGINT       NOT NULL DEFAULT 0,
    username   VARCHAR(32)  NOT NULL,
    nickname   VARCHAR(32)  NOT NULL,
    password   VARCHAR(100) NOT NULL,
    email      VARCHAR(64),
    phone      VARCHAR(16),
    gender     SMALLINT     NOT NULL DEFAULT 0,
    status     SMALLINT     NOT NULL DEFAULT 1,
    login_at   BIGINT,
    remark     VARCHAR(255),
    created_at BIGINT       NOT NULL,
    updated_at BIGINT       NOT NULL,
    CONSTRAINT uk_sys_user_username UNIQUE (username)
);

-- 角色表
CREATE TABLE IF NOT EXISTS sys_role (
    role_id    BIGSERIAL    PRIMARY KEY,
    role_name  VARCHAR(32)  NOT NULL,
    role_key   VARCHAR(64)  NOT NULL,
    role_sort  INT          NOT NULL DEFAULT 0,
    status     SMALLINT     NOT NULL DEFAULT 1,
    data_scope SMALLINT     NOT NULL DEFAULT 1,
    remark     VARCHAR(255),
    created_at BIGINT       NOT NULL,
    updated_at BIGINT       NOT NULL,
    CONSTRAINT uk_sys_role_key UNIQUE (role_key)
);

-- 菜单/权限表（menu_type: M 目录 / C 菜单 / F 按钮；parent_id=0 为顶级）
CREATE TABLE IF NOT EXISTS sys_menu (
    menu_id    BIGSERIAL    PRIMARY KEY,
    parent_id  BIGINT       NOT NULL DEFAULT 0,
    menu_name  VARCHAR(32)  NOT NULL,
    menu_type  CHAR(1)      NOT NULL,
    path       VARCHAR(128),
    perms      VARCHAR(64),
    icon       VARCHAR(64),
    order_num  INT          NOT NULL DEFAULT 0,
    visible    SMALLINT     NOT NULL DEFAULT 1,
    status     SMALLINT     NOT NULL DEFAULT 1
);

-- 用户-角色关系
CREATE TABLE IF NOT EXISTS sys_user_role (
    user_id BIGINT NOT NULL,
    role_id BIGINT NOT NULL,
    PRIMARY KEY (user_id, role_id)
);

-- 角色-菜单关系
CREATE TABLE IF NOT EXISTS sys_role_menu (
    role_id BIGINT NOT NULL,
    menu_id BIGINT NOT NULL,
    PRIMARY KEY (role_id, menu_id)
);
