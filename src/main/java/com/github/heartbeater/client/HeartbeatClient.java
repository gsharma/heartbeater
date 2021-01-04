package com.github.heartbeater.client;

import com.github.heartbeater.lib.Lifecycle;
import com.github.heartbeater.rpc.HeartbeatMessage;
import com.github.heartbeater.rpc.HeartbeatResponse;

/**
 * Client interface for the heartbeater.
 */
public interface HeartbeatClient extends Lifecycle {
    HeartbeatResponse heartbeat(final HeartbeatMessage heartbeatMessage) throws HeartbeatClientException;

    static HeartbeatClient getClient(final String serverHost, final int serverPort, final long serverDeadlineSeconds, final int workerCount) {
        return new HeartbeatClientImpl(serverHost, serverPort, serverDeadlineSeconds, workerCount);
    }

}
