package com.github.heartbeater.server;

import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.heartbeater.rpc.DeregisterPeerRequest;
import com.github.heartbeater.rpc.DeregisterPeerResponse;
import com.github.heartbeater.rpc.HeartbeatMessage;
import com.github.heartbeater.rpc.HeartbeatResponse;
import com.github.heartbeater.rpc.HeartbeatServiceGrpc.HeartbeatServiceImplBase;
import com.github.heartbeater.rpc.RegisterPeerRequest;
import com.github.heartbeater.rpc.RegisterPeerResponse;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;

/**
 * RPC Service implementation for the Heartbeater.
 */
public final class HeartbeatServiceImpl extends HeartbeatServiceImplBase {
    private static final Logger logger = LogManager.getLogger(HeartbeatServiceImpl.class.getSimpleName());

    private final HeartbeatPersister persister;

    // TODO
    private String serverId = UUID.randomUUID().toString();
    private int serverEpoch = 0;

    public HeartbeatServiceImpl(final HeartbeatPersister persister) {
        this.persister = persister;
    }

    @Override
    public void heartbeat(final HeartbeatMessage request,
            final StreamObserver<HeartbeatResponse> responseObserver) {
        // TODO
        // try {
        final int clientEpoch = request.getClientEpoch();
        final String clientId = request.getClientId();
        final HeartbeatResponse response = HeartbeatResponse.newBuilder().setServerId(serverId).setServerEpoch(serverEpoch).build();
        logger.info("heartbeat::[client[id:{}, epoch:{}], server[id:{}, epoch:{}]]", clientId, clientEpoch, serverId, serverEpoch);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        // } catch (HeartbeatServerException heartbeatServerProblem) {
        // responseObserver.onError(toStatusRuntimeException(heartbeatServerProblem));
        // }
    }

    @Override
    public void registerPeer(final RegisterPeerRequest request,
            final StreamObserver<RegisterPeerResponse> responseObserver) {
        // TODO
        try {
            final String peerIp = request.getPeerIp();
            final int peerPort = request.getPeerPort();
            final String peerServerId = request.getPeerServerId();

            final RegisterPeerResponse response = RegisterPeerResponse.newBuilder().setServerId(serverId).setServerEpoch(serverEpoch).build();
            logger.info("registerPeer::[ip:{}, port:{}, serverId:{}]", peerIp, peerPort, peerServerId);

            persister.savePeer(peerServerId, peerIp, peerPort);

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (HeartbeatServerException heartbeatServerProblem) {
            responseObserver.onError(toStatusRuntimeException(heartbeatServerProblem));
        }
    }

    @Override
    public void deregisterPeer(final DeregisterPeerRequest request,
            final StreamObserver<DeregisterPeerResponse> responseObserver) {
        try {
            final String peerServerId = request.getPeerServerId();

            final DeregisterPeerResponse response = DeregisterPeerResponse.newBuilder().setServerId(serverId).setServerEpoch(serverEpoch).build();
            logger.info("deregisterPeer::[serverId:{}]", peerServerId);

            persister.removePeer(peerServerId);

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (HeartbeatServerException heartbeatServerProblem) {
            responseObserver.onError(toStatusRuntimeException(heartbeatServerProblem));
        }
    }

    private static StatusRuntimeException toStatusRuntimeException(final HeartbeatServerException serverException) {
        return new StatusRuntimeException(Status.fromCode(Code.INTERNAL).withCause(serverException).withDescription(serverException.getMessage()));
    }

}
