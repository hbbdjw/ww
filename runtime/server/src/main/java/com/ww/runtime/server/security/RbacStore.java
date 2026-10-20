package com.ww.runtime.server.security;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * RBAC 仓库端口：用户/部门/角色/菜单及其关系的持久化抽象。
 *
 * <p>jdbc 与 memory 两种实现同构，由宿主装配层按 {@code runtime.store} 选择；
 * 领域对象均为不可变 record，写接口以完整对象覆盖（updatedAt 由调用方给定）。</p>
 */
public interface RbacStore {

    // ------------------------------------------------------------------
    // 部门
    // ------------------------------------------------------------------

    long createDept(Dept dept);

    void updateDept(Dept dept);

    Optional<Dept> findDeptById(long deptId);

    List<Dept> listDepts();

    void deleteDept(long deptId);

    // ------------------------------------------------------------------
    // 用户
    // ------------------------------------------------------------------

    long createUser(UserAccount user);

    void updateUser(UserAccount user);

    Optional<UserAccount> findUserById(long userId);

    Optional<UserAccount> findUserByUsername(String username);

    /** 全量用户（keyword 模糊匹配用户名/昵称，null 不过滤；deptId 精确过滤，null 不过滤）。 */
    List<UserAccount> listUsers(String keyword, Long deptId);

    void deleteUser(long userId);

    void updateLoginAt(long userId, long loginAt);

    // ------------------------------------------------------------------
    // 用户-角色
    // ------------------------------------------------------------------

    /** 覆盖式绑定用户角色（先清空再写入）。 */
    void bindUserRoles(long userId, List<Long> roleIds);

    List<Long> findRoleIdsByUser(long userId);

    List<Long> findUserIdsByRole(long roleId);

    // ------------------------------------------------------------------
    // 角色
    // ------------------------------------------------------------------

    long createRole(Role role);

    void updateRole(Role role);

    Optional<Role> findRoleById(long roleId);

    Optional<Role> findRoleByKey(String roleKey);

    List<Role> listRoles();

    void deleteRole(long roleId);

    // ------------------------------------------------------------------
    // 角色-菜单
    // ------------------------------------------------------------------

    /** 覆盖式绑定角色菜单（先清空再写入）。 */
    void bindRoleMenus(long roleId, List<Long> menuIds);

    List<Long> findMenuIdsByRole(long roleId);

    // ------------------------------------------------------------------
    // 菜单
    // ------------------------------------------------------------------

    long createMenu(Menu menu);

    Optional<Menu> findMenuById(long menuId);

    List<Menu> listMenus();

    // ------------------------------------------------------------------
    // 权限聚合
    // ------------------------------------------------------------------

    /** 加载用户的角色 key 与全部菜单权限标识（经角色→菜单聚合，去重）。 */
    UserPermissions loadPermissions(long userId);

    /** 部门。 */
    record Dept(long deptId,
                long parentId,
                String deptName,
                int orderNum,
                String leader,
                String phone,
                int status,
                long createdAt,
                long updatedAt) {
    }

    /** 用户账号（password 为 BCrypt 密文）。 */
    record UserAccount(long userId,
                       long deptId,
                       String username,
                       String nickname,
                       String password,
                       String email,
                       String phone,
                       int gender,
                       int status,
                       Long loginAt,
                       String remark,
                       long createdAt,
                       long updatedAt) {

        /** 创建/更新时携带新密文与时间戳的便捷副本。 */
        public UserAccount withPasswordAndTimestamps(String encodedPassword, long now) {
            return new UserAccount(userId, deptId, username, nickname, encodedPassword,
                    email, phone, gender, status, loginAt, remark, createdAt, now);
        }
    }

    /** 角色。 */
    record Role(long roleId,
                String roleName,
                String roleKey,
                int roleSort,
                int status,
                int dataScope,
                String remark,
                long createdAt,
                long updatedAt) {
    }

    /** 菜单（menuType: M 目录 / C 菜单 / F 按钮）。 */
    record Menu(long menuId,
                long parentId,
                String menuName,
                String menuType,
                String path,
                String perms,
                String icon,
                int orderNum,
                int visible,
                int status) {
    }

    /** 用户权限聚合：角色 key 列表 + 权限标识列表。 */
    record UserPermissions(List<String> roleKeys, List<String> perms) {

        public static UserPermissions empty() {
            return new UserPermissions(List.of(), List.of());
        }

        /** 从缓存 Map（roleKeys/perms 均为字符串列表）重建。 */
        @SuppressWarnings("unchecked")
        public static UserPermissions fromMap(Map<?, ?> map) {
            Object roleKeys = map.get("roleKeys");
            Object perms = map.get("perms");
            return new UserPermissions(
                    List.copyOf(roleKeys instanceof List ? (List<String>) roleKeys : List.of()),
                    List.copyOf(perms instanceof List ? (List<String>) perms : List.of()));
        }
    }
}
