package com.github.heartbeater.server;

import java.util.BitSet;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.heartbeater.client.HeartbeatClient;
import com.github.heartbeater.rpc.HeartbeatMessage;
import com.github.heartbeater.rpc.HeartbeatResponse;

/**
 * Core daemon responsible for heartbeating.
 */
final class HeartbeatWorker extends Thread {
    private static final Logger logger = LogManager.getLogger(HeartbeatWorker.class.getSimpleName());

    private final String identity = UUID.randomUUID().toString();

    private final String serverId;
    private final int serverEpoch;

    private final long runIntervalMillis;
    private final HeartbeatClient client;

    private final int lastHeartbeatsToTrack;
    private final BitSet lastHeartbeatStatuses;
    private long heartbeatSuccesses, heartbeatFailures;

    HeartbeatWorker(final String serverId, final int serverEpoch, final long runIntervalMillis, final String peerHost, final int peerPort,
            final int serverDeadlineMillis, final int lastHeartbeatsToTrack) {
        setDaemon(true);
        setName("heartbeater-" + identity);
        this.serverId = serverId;
        this.serverEpoch = serverEpoch;

        this.runIntervalMillis = runIntervalMillis;
        this.lastHeartbeatsToTrack = lastHeartbeatsToTrack;
        this.lastHeartbeatStatuses = new BitSet();

        final int clientWorkerCount = 1;
        client = HeartbeatClient.getClient(peerHost, peerPort, serverDeadlineMillis, clientWorkerCount);

        logger.info("Starting HeartbeatWorker [{}], serverId:{}, serverEpoch:{}, runIntervalMillis:{}, lastHeartbeatsToTrack:{}", identity, serverId,
                serverEpoch, runIntervalMillis, lastHeartbeatsToTrack);
        start();
    }

    String getIdentity() {
        return identity;
    }

    BitSet getLastHeartbeatStatuses() {
        return lastHeartbeatStatuses;
    }

    @Override
    public void run() {
        if (client != null && !client.isRunning()) {
            try {
                client.start();
            } catch (Exception heartbeatClientProblem) {
                // TODO
                logger.error("Failed to start heartbeat client", heartbeatClientProblem);
                return;
            }
        }
        int heartbeatCounter = 0;
        logger.info("Started HeartbeatWorker [{}]", identity);
        while (!isInterrupted()) {
            try {
                int sourceEpoch = 0;
                String sourceClientId = null;
                try {
                    sourceEpoch = serverEpoch;
                    sourceClientId = serverId;
                    final HeartbeatMessage heartbeatMessage = HeartbeatMessage.newBuilder().setClientEpoch(sourceEpoch).setClientId(sourceClientId)
                            .build();
                    heartbeatCounter = ++heartbeatCounter % lastHeartbeatsToTrack;
                    final HeartbeatResponse heartbeatResponse = client.heartbeat(heartbeatMessage);
                    logger.info("heartbeat::[request[id:{}, epoch:{}], response[id:{}, epoch:{}]]", heartbeatMessage.getClientId(),
                            heartbeatMessage.getClientEpoch(),
                            heartbeatResponse.getServerId(), heartbeatResponse.getServerEpoch());
                    lastHeartbeatStatuses.set(heartbeatCounter, true);
                    logger.info("counter:{}, {}", heartbeatCounter, lastHeartbeatStatuses.get(heartbeatCounter));
                    heartbeatSuccesses++;
                } catch (final Throwable heartbeatProblem) {
                    logger.error(String.format("Failed heartbeat::[request[id:%s, epoch:%s]]", sourceClientId, sourceEpoch), heartbeatProblem);
                    lastHeartbeatStatuses.set(heartbeatCounter, false);
                    logger.info("counter:{}, {}", heartbeatCounter, lastHeartbeatStatuses.get(heartbeatCounter));
                    heartbeatFailures++;
                }
                sleep(runIntervalMillis);
            } catch (final InterruptedException interrupted) {
                break;
            }
        }
        if (client != null && client.isRunning()) {
            try {
                client.stop();
            } catch (Exception heartbeatClientProblem) {
                // TODO
                logger.error("Failed to stop heartbeat client", heartbeatClientProblem);
            }
        }
        logger.info("Stopped HeartbeatWorker [{}], heartbeats::[successes:{}, failures:{}]", identity, heartbeatSuccesses, heartbeatFailures);
        logger.info("Stopped HeartbeatWorker [{}], recentHeartbeats::{}", identity, lastHeartbeatStatuses.size());
    }

}
