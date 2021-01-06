package com.github.heartbeater.server;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private final String serverId;
    private final int serverEpoch;

    private Map<String, HeartbeatWorker> heartbeatWorkers;

    private Map<String, Set<Heartbeat>> heartbeatsReceived;

    private final ReentrantReadWriteLock peerEditLock = new ReentrantReadWriteLock(true);

    HeartbeatServiceImpl(final String serverId, final int serverEpoch) {
        this.serverId = serverId;
        this.serverEpoch = serverEpoch;
    }

    @Override
    public void heartbeat(final HeartbeatMessage request,
            final StreamObserver<HeartbeatResponse> responseObserver) {
        try {
            final String clientId = request.getClientId();
            Set<Heartbeat> heartbeats = heartbeatsReceived.get(clientId);
            if (heartbeats == null) {
                final int heartbeatsToKeep = 50;
                heartbeats = Collections.newSetFromMap(new LinkedHashMap<Heartbeat, Boolean>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean removeEldestEntry(final Map.Entry<Heartbeat, Boolean> eldest) {
                        final boolean removalCheckPassed = size() > heartbeatsToKeep;
                        // if (removalCheckPassed) {
                        // logger.info("LRU trimming heartbeats for {} to {}", clientId, heartbeatsToKeep);
                        // }
                        return removalCheckPassed;
                    }
                });
                logger.info("Setting up to receive first heartbeat for {}", clientId);
                heartbeatsReceived.putIfAbsent(clientId, heartbeats);
            }

            final int clientEpoch = request.getClientEpoch();
            final long receivedTstamp = Instant.now().toEpochMilli();

            // TODO: persist: <receivedTstamp, clientEpoch> to heartbeat-<clientId>
            logger.debug("Persisting [{},{}] to heartbeat-{}", receivedTstamp, clientEpoch, clientId);
            heartbeats.add(new Heartbeat(receivedTstamp, clientEpoch));

            final HeartbeatResponse response = HeartbeatResponse.newBuilder().setServerId(serverId).setServerEpoch(serverEpoch).build();
            logger.debug("heartbeat::[client[id:{}, epoch:{}], server[id:{}, epoch:{}]]", clientId, clientEpoch, serverId, serverEpoch);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception heartbeatServerProblem) {
            responseObserver.onError(toStatusRuntimeException(heartbeatServerProblem));
        }
    }

    @Override
    public void registerPeer(final RegisterPeerRequest request,
            final StreamObserver<RegisterPeerResponse> responseObserver) {
        if (peerEditLock.writeLock().tryLock()) {
            try {
                final String peerHost = request.getPeerHost();
                final int peerPort = request.getPeerPort();
                final String peerServerId = request.getPeerId();
                final int heartbeatFreqMillis = request.getHeartbeatFreqMillis();

                if (!heartbeatWorkers.containsKey(peerServerId)) {
                    persister.savePeer(peerServerId, peerHost, peerPort);

                    final long runIntervalMillis = Math.max(heartbeatFreqMillis, 5L);
                    final HeartbeatWorker heartbeatWorker = new HeartbeatWorker(serverId, serverEpoch, runIntervalMillis, peerHost, peerPort);
                    heartbeatWorkers.put(peerServerId, heartbeatWorker);
                } else {
                    logger.warn("{} is an already registered peer", peerServerId);
                }

                final RegisterPeerResponse response = RegisterPeerResponse.newBuilder().setServerId(serverId).setServerEpoch(serverEpoch).build();
                logger.info("registerPeer::[host:{}, port:{}, serverId:{}]", peerHost, peerPort, peerServerId);

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (HeartbeatServerException heartbeatServerProblem) {
                responseObserver.onError(toStatusRuntimeException(heartbeatServerProblem));
            } finally {
                peerEditLock.writeLock().unlock();
            }
        } else {
            logger.warn("Failed to acquire peerEditLock for registerPeer");
            // TODO: responseObserver.onError(null);
        }
    }

    @Override
    public void deregisterPeer(final DeregisterPeerRequest request,
            final StreamObserver<DeregisterPeerResponse> responseObserver) {
        if (peerEditLock.writeLock().tryLock()) {
            try {
                final String peerServerId = request.getPeerId();

                if (heartbeatWorkers.containsKey(peerServerId)) {
                    persister.removePeer(peerServerId);

                    final HeartbeatWorker heartbeatWorker = heartbeatWorkers.remove(peerServerId);
                    heartbeatWorker.interrupt();
                } else {
                    logger.warn("{} is not a registered peer", peerServerId);
                }

                final DeregisterPeerResponse response = DeregisterPeerResponse.newBuilder().setServerId(serverId).setServerEpoch(serverEpoch).build();
                logger.info("deregisterPeer::[peerId:{}]", peerServerId);

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (HeartbeatServerException heartbeatServerProblem) {
                responseObserver.onError(toStatusRuntimeException(heartbeatServerProblem));
            } finally {
                peerEditLock.writeLock().unlock();
            }
        } else {
            logger.warn("Failed to acquire peerEditLock for deregisterPeer");
            // TODO: responseObserver.onError(null);
        }
    }

    private static StatusRuntimeException toStatusRuntimeException(final Exception serverException) {
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

            heartbeatWorkers = new ConcurrentHashMap<>();
            heartbeatsReceived = new ConcurrentHashMap<>();

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
            heartbeatWorkers.clear();
            for (final Set<Heartbeat> heartbeats : heartbeatsReceived.values()) {
                heartbeats.clear();
            }
            heartbeatsReceived.clear();

            logger.info("Stopped HeartbeatService [{}]", serverId);
        } else {
            logger.error("Invalid attempt to stop an already stopped HeartbeatService");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    // a simple heartbeat struct
    private final static class Heartbeat {
        private final long receivedTstamp;
        private final int clientEpoch;

        private Heartbeat(final long receivedTstamp, final int clientEpoch) {
            this.receivedTstamp = receivedTstamp;
            this.clientEpoch = clientEpoch;
        }
    }

}
