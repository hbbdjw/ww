package com.ww.admin.controller;

import java.util.Date;

import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ww.admin.common.JsonResult;
import com.ww.admin.entity.SysLog;
import com.ww.admin.service.LogService;
import com.ww.common.query.Page;
import com.ww.common.query.Pageable;

/**
 * 日志控制器：历史日志分页筛选 + 按时间段删除。所有接口均需 {@code log:*} 权限。
 * <p>时间参数统一为 epoch 毫秒时间戳，由前端时间选择器直接透传。
 */
@RestController
@RequestMapping("/log")
@RequiresPermissions("log:*")
public class LogController {

    private final LogService logService;

    public LogController(LogService logService) {
        this.logService = logService;
    }

    /**
     * 分页查询日志（按级别、节点、关键字、时间段筛选）。
     */
    @GetMapping("/page")
    public JsonResult page(@RequestParam(defaultValue = "1") int pageNumber,
                           @RequestParam(defaultValue = "15") int pageSize,
                           @RequestParam(required = false) String level,
                           @RequestParam(required = false) String node,
                           @RequestParam(required = false) String keyword,
                           @RequestParam(required = false) Long startTime,
                           @RequestParam(required = false) Long endTime) {
        Pageable pageable = new Pageable(pageNumber, pageSize);
        Date start = startTime != null ? new Date(startTime) : null;
        Date end = endTime != null ? new Date(endTime) : null;
        Page<SysLog> page = logService.findPage(level, node, keyword, start, end, pageable);
        return JsonResult.success(page);
    }

    /**
     * 按时间段批量删除日志（含两端边界）。
     *
     * @return data 为删除条数
     */
    @DeleteMapping("/delete")
    public JsonResult delete(@RequestParam Long startTime,
                             @RequestParam Long endTime) {
        int deleted = logService.deleteByTimeRange(new Date(startTime), new Date(endTime));
        return JsonResult.success(deleted, "删除成功");
    }
}
