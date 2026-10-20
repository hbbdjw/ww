package com.ww.runtime.server.web;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ww.runtime.cluster.decommission.DecommissionRecord;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.server.decommission.DecommissionService;
import com.ww.runtime.server.decommission.DecommissionService.DecommissionException;

/**
 * 节点退役 REST 端点（前缀 {@code /rt}）。
 *
 * <ul>
 *   <li><b>POST /rt/nodes/{nodeId}/decommission/precheck</b>：预检（纯读，无副作用）</li>
 *   <li><b>POST /rt/nodes/{nodeId}/decommission</b>：启动退役</li>
 *   <li><b>GET /rt/nodes/{nodeId}/decommission/status</b>：查询退役状态 + 进度</li>
 * </ul>
 */
@RestController
@RequestMapping("/rt")
public class RuntimeDecommissionController {

    private final DecommissionService service;

    public RuntimeDecommissionController(DecommissionService service) {
        this.service = service;
    }

    @PostMapping("/nodes/{nodeId}/decommission/precheck")
    public ResponseEntity<Object> precheck(@PathVariable("nodeId") String nodeId) {
        NodeId node;
        try {
            node = NodeId.of(nodeId);
        } catch (RuntimeException e) {
            return badRequest(e.getMessage());
        }
        try {
            return ResponseEntity.ok(service.precheck(node));
        } catch (DecommissionException e) {
            return status(e.httpStatus(), e.getMessage());
        }
    }

    @PostMapping("/nodes/{nodeId}/decommission")
    public ResponseEntity<Object> start(@PathVariable("nodeId") String nodeId,
                                        @RequestBody(required = false) DecommissionRequest body) {
        NodeId node;
        try {
            node = NodeId.of(nodeId);
        } catch (RuntimeException e) {
            return badRequest(e.getMessage());
        }
        boolean force = body != null && body.force();
        try {
            DecommissionRecord record = service.start(node, force);
            int httpStatus = record.state().terminal() ? 200 : 202;
            return ResponseEntity.status(httpStatus).body(Map.of(
                    "nodeId", record.node().value(),
                    "state", record.state().name(),
                    "force", record.force(),
                    "startedAtMillis", record.startedAtMillis()));
        } catch (DecommissionException e) {
            return status(e.httpStatus(), e.getMessage());
        }
    }

    @GetMapping("/nodes/{nodeId}/decommission/status")
    public ResponseEntity<Object> status(@PathVariable("nodeId") String nodeId) {
        NodeId node;
        try {
            node = NodeId.of(nodeId);
        } catch (RuntimeException e) {
            return badRequest(e.getMessage());
        }
        return ResponseEntity.ok(service.progress(node));
    }

    public record DecommissionRequest(boolean force) {
    }

    private static ResponseEntity<Object> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("message", String.valueOf(message)));
    }

    private static ResponseEntity<Object> status(int httpStatus, String message) {
        return ResponseEntity.status(httpStatus).body(Map.of("message", String.valueOf(message)));
    }
}
