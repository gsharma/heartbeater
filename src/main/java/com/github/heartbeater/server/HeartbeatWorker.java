package com.github.heartbeater.server;

import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.heartbeater.client.HeartbeatClient;
import com.github.heartbeater.client.HeartbeatClientException;
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
    private final String peerHost;
    private final int peerPort;
    private final HeartbeatClient client;

    private long heartbeatCounter;

    HeartbeatWorker(final String serverId, final int serverEpoch, final long runIntervalMillis, final String peerHost, final int peerPort) {
        setDaemon(true);
        setName("heartbeater-" + identity);
        this.serverId = serverId;
        this.serverEpoch = serverEpoch;

        this.runIntervalMillis = runIntervalMillis;
        this.peerHost = peerHost;
        this.peerPort = peerPort;

        final long serverDeadlineSeconds = 1L;
        final int clientWorkerCount = 1;
        client = HeartbeatClient.getClient(peerHost, peerPort, serverDeadlineSeconds, clientWorkerCount);

        start();
    }

    String getIdentity() {
        return identity;
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
        logger.info("Started HeartbeatWorker [{}]", identity);
        while (!isInterrupted()) {
            try {
                try {
                    final HeartbeatMessage heartbeatRequest = HeartbeatMessage.newBuilder().setClientEpoch(serverEpoch).setClientId(serverId).build();
                    final HeartbeatResponse heartbeatResponse = client.heartbeat(heartbeatRequest);
                    heartbeatCounter++;
                } catch (HeartbeatClientException heartbeatClientProblem) {
                    // TODO
                    logger.error(heartbeatClientProblem);
                }
                sleep(runIntervalMillis);
            } catch (InterruptedException interrupted) {
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
        logger.info("Stopped HeartbeatWorker [{}], heartbeatCounter:{} ", identity, heartbeatCounter);
    }

}
