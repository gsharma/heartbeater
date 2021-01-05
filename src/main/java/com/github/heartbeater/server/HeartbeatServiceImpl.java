package com.github.heartbeater.server;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.heartbeater.lib.Lifecycle;
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
final class HeartbeatServiceImpl extends HeartbeatServiceImplBase implements Lifecycle {
    private static final Logger logger = LogManager.getLogger(HeartbeatServiceImpl.class.getSimpleName());

    private final AtomicBoolean running = new AtomicBoolean(false);

    private HeartbeatPersister persister;

    // TODO
    private String serverId = UUID.randomUUID().toString();
    private int serverEpoch = 0;

    private Map<String, HeartbeatWorker> heartbeatWorkers;

    HeartbeatServiceImpl() {
    }

    @Override
    public void heartbeat(final HeartbeatMessage request,
            final StreamObserver<HeartbeatResponse> responseObserver) {
        // TODO
        // try {
        final int clientEpoch = request.getClientEpoch();
        final String clientId = request.getClientId();
        final long receivedTstamp = Instant.now().toEpochMilli();
        
        // TODO: persist: <receivedTstamp, clientEpoch> to heartbeat-<clientId>
        
        final HeartbeatResponse response = HeartbeatResponse.newBuilder().setServerId(serverId).setServerEpoch(serverEpoch).build();
        logger.debug("heartbeat::[client[id:{}, epoch:{}], server[id:{}, epoch:{}]]", clientId, clientEpoch, serverId, serverEpoch);
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
            final String peerHost = request.getPeerHost();
            final int peerPort = request.getPeerPort();
            final String peerServerId = request.getPeerId();

            final RegisterPeerResponse response = RegisterPeerResponse.newBuilder().setServerId(serverId).setServerEpoch(serverEpoch).build();
            logger.info("registerPeer::[host:{}, port:{}, serverId:{}]", peerHost, peerPort, peerServerId);

            persister.savePeer(peerServerId, peerHost, peerPort);

            final long runIntervalMillis = 100L;
            final HeartbeatWorker heartbeatWorker = new HeartbeatWorker(serverId, serverEpoch, runIntervalMillis, peerHost, peerPort);
            heartbeatWorkers.put(peerServerId, heartbeatWorker);

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
            final String peerServerId = request.getPeerId();

            final DeregisterPeerResponse response = DeregisterPeerResponse.newBuilder().setServerId(serverId).setServerEpoch(serverEpoch).build();
            logger.info("deregisterPeer::[peerId:{}]", peerServerId);

            persister.removePeer(peerServerId);

            final HeartbeatWorker heartbeatWorker = heartbeatWorkers.remove(peerServerId);
            heartbeatWorker.interrupt();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (HeartbeatServerException heartbeatServerProblem) {
            responseObserver.onError(toStatusRuntimeException(heartbeatServerProblem));
        }
    }

    private static StatusRuntimeException toStatusRuntimeException(final HeartbeatServerException serverException) {
        return new StatusRuntimeException(Status.fromCode(Code.INTERNAL).withCause(serverException).withDescription(serverException.getMessage()));
    }

    @Override
    public String getIdentity() {
        return serverId;
    }

    @Override
    public void start() throws Exception {
        if (running.compareAndSet(false, true)) {
            persister = HeartbeatPersister.getPersister();
            persister.start();

            this.heartbeatWorkers = new ConcurrentHashMap<>();

            logger.info("Started HeartbeatService [{}]", serverId);
        } else {
            logger.error("Invalid attempt to start an already running HeartbeatService");
        }
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping HeartbeatService [{}]", serverId);
        if (running.compareAndSet(true, false)) {
            for (final HeartbeatWorker worker : heartbeatWorkers.values()) {
                worker.interrupt();
            }
            if (persister.isRunning()) {
                persister.stop();
            }
            logger.info("Stopped HeartbeatService [{}]", serverId);
        } else {
            logger.error("Invalid attempt to stop an already stopped HeartbeatService");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

}
