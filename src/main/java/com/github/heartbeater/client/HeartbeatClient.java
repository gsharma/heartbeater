package com.github.heartbeater.client;

import com.github.heartbeater.lib.Lifecycle;
import com.github.heartbeater.rpc.DeregisterPeerRequest;
import com.github.heartbeater.rpc.DeregisterPeerResponse;
import com.github.heartbeater.rpc.HeartbeatMessage;
import com.github.heartbeater.rpc.HeartbeatResponse;
import com.github.heartbeater.rpc.RegisterPeerRequest;
import com.github.heartbeater.rpc.RegisterPeerResponse;

/**
 * Client interface for the heartbeater.
 */
public interface HeartbeatClient extends Lifecycle {

    RegisterPeerResponse registerPeer(final RegisterPeerRequest registerPeerRequest) throws HeartbeatClientException;

    DeregisterPeerResponse deregisterPeer(final DeregisterPeerRequest deregisterPeerRequest) throws HeartbeatClientException;

    HeartbeatResponse heartbeat(final HeartbeatMessage heartbeatMessage) throws HeartbeatClientException;

    static HeartbeatClient getClient(final String serverHost, final int serverPort, final long serverDeadlineSeconds, final int workerCount) {
        return new HeartbeatClientImpl(serverHost, serverPort, serverDeadlineSeconds, workerCount);
    }

}
