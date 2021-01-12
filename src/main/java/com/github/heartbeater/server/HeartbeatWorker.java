package com.github.heartbeater.server;

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
    private final String peerHost;
    private final int peerPort;
    private final HeartbeatClient client;

    private long heartbeatSuccesses, heartbeatFailures;

    HeartbeatWorker(final String serverId, final int serverEpoch, final long runIntervalMillis, final String peerHost, final int peerPort,
            final int serverDeadlineMillis) {
        setDaemon(true);
        setName("heartbeater-" + identity);
        this.serverId = serverId;
        this.serverEpoch = serverEpoch;

        this.runIntervalMillis = runIntervalMillis;
        this.peerHost = peerHost;
        this.peerPort = peerPort;

        final int clientWorkerCount = 1;
        client = HeartbeatClient.getClient(peerHost, peerPort, serverDeadlineMillis, clientWorkerCount);

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
                int sourceEpoch = 0;
                String sourceClientId = null;
                try {
                    sourceEpoch = serverEpoch;
                    sourceClientId = serverId;
                    final HeartbeatMessage heartbeatMessage = HeartbeatMessage.newBuilder().setClientEpoch(sourceEpoch).setClientId(sourceClientId)
                            .build();
                    final HeartbeatResponse heartbeatResponse = client.heartbeat(heartbeatMessage);
                    logger.info("heartbeat::[request[id:{}, epoch:{}], response[id:{}, epoch:{}]]", heartbeatMessage.getClientId(),
                            heartbeatMessage.getClientEpoch(),
                            heartbeatResponse.getServerId(), heartbeatResponse.getServerEpoch());
                    heartbeatSuccesses++;
                } catch (Throwable heartbeatProblem) {
                    logger.error("heartbeat::[request[id:{}, epoch:{}], response[{}]]", sourceClientId, sourceEpoch, heartbeatProblem);
                    heartbeatFailures++;
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
        logger.info("Stopped HeartbeatWorker [{}], heartbeats::[successes:{}, failures:{}]", identity, heartbeatSuccesses, heartbeatFailures);
    }

}
