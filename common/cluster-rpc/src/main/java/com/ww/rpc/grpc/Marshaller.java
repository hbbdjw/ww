package com.ww.rpc.grpc;

import java.util.HashMap;
import java.util.Map;

import com.ww.rpc.core.RpcRequest;
import com.ww.rpc.core.RpcResponse;
import com.ww.rpc.grpc.proto.RpcMessage;
import com.ww.rpc.grpc.proto.RpcResponseMessage;

/**
 * Marshaller：在 RpcRequest/RpcResponse（Java 对象）与 RpcMessage/RpcResponseMessage（Protobuf）之间双向转换。
 */
final class Marshaller {

    private Marshaller() {}

    static RpcMessage toProto(RpcRequest request) {
        RpcMessage.Builder builder = RpcMessage.newBuilder()
                .setRequestId(request.getRequestId())
                .setServiceName(request.getServiceName())
                .setTimeoutMs(0);
        if (request.getPayload() != null) {
            builder.setPayload(com.google.protobuf.ByteString.copyFrom(request.getPayload()));
        }
        for (Map.Entry<String, String> e : request.getHeaders().entrySet()) {
            builder.putHeaders(e.getKey(), e.getValue());
        }
        return builder.build();
    }

    static RpcRequest fromProto(RpcMessage proto) {
        Map<String, String> headers = proto.getHeadersMap();
        return RpcRequest.newBuilder()
                .requestId(proto.getRequestId())
                .serviceName(proto.getServiceName())
                .payload(proto.getPayload().toByteArray())
                .headers(new HashMap<>(headers))
                .build();
    }

    static RpcResponseMessage toProto(RpcResponse response) {
        RpcResponseMessage.Builder builder = RpcResponseMessage.newBuilder()
                .setRequestId(response.getRequestId())
                .setStatus(response.getStatus().getCode());
        if (response.getPayload() != null) {
            builder.setPayload(com.google.protobuf.ByteString.copyFrom(response.getPayload()));
        }
        if (response.getLeaderHint() != null) {
            builder.setLeaderHint(response.getLeaderHint());
        }
        if (response.getErrorMessage() != null) {
            builder.setErrorMessage(response.getErrorMessage());
        }
        for (Map.Entry<String, String> e : response.getHeaders().entrySet()) {
            builder.putHeaders(e.getKey(), e.getValue());
        }
        return builder.build();
    }

    static RpcResponse fromProto(RpcResponseMessage proto) {
        RpcResponse.Builder builder = RpcResponse.newBuilder()
                .requestId(proto.getRequestId())
                .status(RpcResponse.Status.fromCode(proto.getStatus()))
                .payload(proto.getPayload().toByteArray());
        if (!proto.getLeaderHint().isEmpty()) {
            builder.leaderHint(proto.getLeaderHint());
        }
        if (!proto.getErrorMessage().isEmpty()) {
            builder.errorMessage(proto.getErrorMessage());
        }
        builder.headers(new HashMap<>(proto.getHeadersMap()));
        return builder.build();
    }
}
