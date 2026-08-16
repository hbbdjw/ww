package com.ww.admin.service;

import java.util.Date;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.querydsl.jpa.impl.JPAQuery;
import com.ww.admin.entity.QSysLog;
import com.ww.admin.entity.SysLog;
import com.ww.common.dao.BaseDaoImpl;
import com.ww.common.query.Page;
import com.ww.common.query.Pageable;

/**
 * 系统日志查询/删除服务。
 * <p>
 * 日志为纯追加数据，查询走 QueryDSL 动态条件分页；删除走 QueryDSL bulk delete（DML，绕过一级缓存），
 * 按时间段批量清除。
 */
@Service
public class LogService {

    private final BaseDaoImpl baseDao;

    public LogService(BaseDaoImpl baseDao) {
        this.baseDao = baseDao;
    }

    /**
     * 按日志级别、集群节点、关键字、时间段分页查询日志。
     *
     * @param level     日志级别（TRACE/DEBUG/INFO/WARN/ERROR，可选）
     * @param node      集群节点标识 host:port（可选，精确匹配）
     * @param keyword   关键字（可选，模糊匹配 message/loggerName）
     * @param startTime 起始时间（含，可选）
     * @param endTime   结束时间（含，可选）
     */
    public Page<SysLog> findPage(String level, String node, String keyword,
                                 Date startTime, Date endTime, Pageable pageable) {
        QSysLog l = QSysLog.sysLog;
        JPAQuery<SysLog> q = baseDao.queryFactory()
                .selectFrom(l)
                .orderBy(l.createdDate.desc());

        if (level != null && !level.isEmpty()) {
            q.where(l.level.eq(level));
        }
        if (node != null && !node.isEmpty()) {
            q.where(l.node.eq(node));
        }
        if (keyword != null && !keyword.isEmpty()) {
            q.where(l.message.containsIgnoreCase(keyword)
                    .or(l.loggerName.containsIgnoreCase(keyword)));
        }
        if (startTime != null) {
            q.where(l.createdDate.goe(startTime));
        }
        if (endTime != null) {
            q.where(l.createdDate.loe(endTime));
        }
        return baseDao.findPage(q.getType(), pageable);
    }

    /**
     * 按时间段批量删除日志（含两端边界）。
     *
     * @return 删除的记录条数
     */
    @Transactional
    public int deleteByTimeRange(Date startTime, Date endTime) {
        if (startTime == null || endTime == null) {
            throw new IllegalArgumentException("删除日志需同时指定起始与结束时间");
        }
        if (startTime.after(endTime)) {
            throw new IllegalArgumentException("起始时间不能晚于结束时间");
        }
        QSysLog l = QSysLog.sysLog;
        long deleted = baseDao.queryFactory()
                .delete(l)
                .where(l.createdDate.between(startTime, endTime))
                .execute();
        return (int) deleted;
    }
}
