package com.github.heartbeater.server;

import java.util.UUID;

import com.github.heartbeater.rpc.HeartbeatMessage;
import com.github.heartbeater.rpc.HeartbeatResponse;
import com.github.heartbeater.rpc.HeartbeatServiceGrpc.HeartbeatServiceImplBase;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;

/**
 * RPC Service implementation for the Heartbeater.
 */
public final class HeartbeatServiceImpl extends HeartbeatServiceImplBase {
    // TODO
    private String serverId = UUID.randomUUID().toString();
    private long serverEpoch = 0L;

    @Override
    public void heartbeat(final HeartbeatMessage request,
            final StreamObserver<HeartbeatResponse> responseObserver) {
        // TODO
        // try {
        final HeartbeatResponse response = HeartbeatResponse.newBuilder().setServerId(serverId).setServerEpoch(serverEpoch).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        // } catch (HeartbeatServerException heartbeatServerProblem) {
        // responseObserver.onError(toStatusRuntimeException(heartbeatServerProblem));
        // }
    }

    private static StatusRuntimeException toStatusRuntimeException(final HeartbeatServerException serverException) {
        return new StatusRuntimeException(Status.fromCode(Code.INTERNAL).withCause(serverException).withDescription(serverException.getMessage()));
    }

}
