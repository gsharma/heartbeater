package com.github.heartbeater;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.github.heartbeater.client.HeartbeatClient;
import com.github.heartbeater.client.HeartbeatClientException;
import com.github.heartbeater.rpc.DeregisterPeerRequest;
import com.github.heartbeater.rpc.DeregisterPeerResponse;
import com.github.heartbeater.rpc.HeartbeatMessage;
import com.github.heartbeater.rpc.HeartbeatResponse;
import com.github.heartbeater.rpc.RegisterPeerRequest;
import com.github.heartbeater.rpc.RegisterPeerResponse;
import com.github.heartbeater.server.HeartbeatServer;
import com.github.heartbeater.server.HeartbeatServer.HeartbeatServerBuilder;

/**
 * End to end tests for keeping heartbeater's sanity.
 */
public final class HeartbeatIntegrationTest {
    private static final Logger logger = LogManager.getLogger(HeartbeatIntegrationTest.class.getSimpleName());

    private final String serverHost = "127.0.0.1";
    private final int serverPort = 8181;
    private final long serverDeadlineSeconds = 1L;
    private final int serverWorkerCount = 1;
    private final int clientWorkerCount = 1;

    private HeartbeatServer server;
    private HeartbeatClient client;

    {
        Thread.currentThread().setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable problem) {
                logger.error("Unexpected error in thread {}", thread.getName(), problem);
            }
        });
    }

    @Before
    public void initClientServer() throws Exception {
        server = HeartbeatServerBuilder.newBuilder().serverHost(serverHost).serverPort(serverPort).workerCount(serverWorkerCount).build();
        server.start();
        assertTrue(server.isRunning());

        client = HeartbeatClient.getClient(serverHost, serverPort, serverDeadlineSeconds, clientWorkerCount);
        client.start();
        assertTrue(client.isRunning());
    }

    @After
    public void tiniClientServer() throws Exception {
        if (client != null) {
            client.stop();
            assertFalse(client.isRunning());
        }
        if (server != null) {
            server.stop();
            assertFalse(server.isRunning());
        }
    }

    @Test
    public void testSimulatedHeartbeat() throws HeartbeatClientException {
        final int clientEpoch = 0;
        final HeartbeatMessage heartbeatRequest = HeartbeatMessage.newBuilder().setClientEpoch(clientEpoch).setClientId(client.getIdentity()).build();
        final HeartbeatResponse heartbeatResponse = client.heartbeat(heartbeatRequest);
        assertNotNull(heartbeatResponse.getServerId());
        assertEquals(clientEpoch, heartbeatResponse.getServerEpoch());

        final String peerIp = "127.0.0.1";
        final int peerPort = 3131;
        final String peerServerId = UUID.randomUUID().toString();

        final RegisterPeerRequest registerPeerRequest = RegisterPeerRequest.newBuilder().setPeerIp(peerIp).setPeerPort(peerPort)
                .setPeerServerId(peerServerId).build();
        final RegisterPeerResponse registerPeerResponse = client.registerPeer(registerPeerRequest);
        assertNotNull(registerPeerResponse.getServerId());

        final DeregisterPeerRequest deregisterPeerRequest = DeregisterPeerRequest.newBuilder().setPeerServerId(peerServerId).build();
        final DeregisterPeerResponse deregisterPeerResponse = client.deregisterPeer(deregisterPeerRequest);
        assertNotNull(deregisterPeerResponse.getServerId());
    }

    @Ignore
    @Test
    public void test2ServerHeartbeating() {
        // TODO
        // setup and start serverOne: serverOne is alive
        // setup and start serverTwo: serverTwo is alive
        // call registerPeer(serverTwo) on serverOne: serverOne->serverTwo heartbeating starts
        // call registerPeer(serverOne) on serverTwo: serverTwo->serverOne heartbeating starts
        // shut e'thing down
    }

}
