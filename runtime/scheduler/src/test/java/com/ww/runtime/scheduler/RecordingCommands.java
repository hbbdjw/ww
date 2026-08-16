package com.ww.runtime.scheduler;

import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** 测试用命令通道：记录全部命令并恒定成功。 */
final class RecordingCommands implements NodeCommandChannel {

    final List<String> calls = new CopyOnWriteArrayList<>();

    @Override
    public boolean startCapability(NodeId node, CapabilityType type) {
        calls.add("start:" + node.value());
        return true;
    }

    @Override
    public boolean stopCapability(NodeId node, CapabilityType type) {
        calls.add("stop:" + node.value());
        return true;
    }

    @Override
    public boolean freeze(NodeId node, CapabilityType type, boolean frozen) {
        calls.add("freeze:" + node.value() + ":" + frozen);
        return true;
    }

    List<String> started() {
        return calls.stream().filter(call -> call.startsWith("start:"))
                .map(call -> call.substring("start:".length()))
                .toList();
    }

    List<String> stopped() {
        return calls.stream().filter(call -> call.startsWith("stop:"))
                .map(call -> call.substring("stop:".length()))
                .toList();
    }

    List<String> freezes() {
        return calls.stream().filter(call -> call.startsWith("freeze:")).toList();
    }
}
