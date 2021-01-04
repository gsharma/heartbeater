package com.github.heartbeater.server;

import com.github.heartbeater.lib.Lifecycle;

public interface HeartbeatPersister extends Lifecycle {
    void savePeer(final String serverId, final String serverIp, final int port) throws HeartbeatServerException;

    void findPeer(final String serverId) throws HeartbeatServerException;

    void removePeer(final String serverId) throws HeartbeatServerException;

    static HeartbeatPersister getPersister() {
        return new HeartbeatPersisterImpl();
    }

}
