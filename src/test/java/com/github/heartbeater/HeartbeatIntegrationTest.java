package com.github.heartbeater;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import com.github.heartbeater.client.HeartbeatClient;
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

    {
        Thread.currentThread().setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable problem) {
                logger.error("Unexpected error in thread {}", thread.getName(), problem);
            }
        });
    }

    @Ignore
    @Test
    public void testSimulatedHeartbeat() throws Exception {
        final String serverHost = "127.0.0.1";
        final int serverPort = 8181;
        final long serverDeadlineSeconds = 1L;
        final int serverWorkerCount = 1;
        final int clientWorkerCount = 1;

        final HeartbeatServer server = HeartbeatServerBuilder.newBuilder().serverHost(serverHost).serverPort(serverPort)
                .workerCount(serverWorkerCount)
                .build();
        server.start();
        assertTrue(server.isRunning());

        final HeartbeatClient client = HeartbeatClient.getClient(serverHost, serverPort, serverDeadlineSeconds, clientWorkerCount);
        client.start();
        assertTrue(client.isRunning());

        final int clientEpoch = 0;
        final HeartbeatMessage heartbeatRequest = HeartbeatMessage.newBuilder().setClientEpoch(clientEpoch).setClientId(client.getIdentity()).build();
        final HeartbeatResponse heartbeatResponse = client.heartbeat(heartbeatRequest);
        assertNotNull(heartbeatResponse.getServerId());
        assertEquals(clientEpoch, heartbeatResponse.getServerEpoch());

        final String peerHost = "127.0.0.1";
        final int peerPort = 3131;
        final String peerServerId = UUID.randomUUID().toString();

        final RegisterPeerRequest registerPeerRequest = RegisterPeerRequest.newBuilder().setPeerHost(peerHost).setPeerPort(peerPort)
                .setPeerId(peerServerId).build();
        final RegisterPeerResponse registerPeerResponse = client.registerPeer(registerPeerRequest);
        assertNotNull(registerPeerResponse.getServerId());

        final DeregisterPeerRequest deregisterPeerRequest = DeregisterPeerRequest.newBuilder().setPeerId(peerServerId).build();
        final DeregisterPeerResponse deregisterPeerResponse = client.deregisterPeer(deregisterPeerRequest);
        assertNotNull(deregisterPeerResponse.getServerId());

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
    public void test2ServerHeartbeating() throws Exception {
        // 1. setup and start serverOne: serverOne is alive
        final String serverOneHost = "127.0.0.1";
        final int serverOnePort = 8181;
        final long serverOneDeadlineSeconds = 1L;
        final int serverOneWorkerCount = 1;
        final int clientOneWorkerCount = 1;

        final HeartbeatServer serverOne = HeartbeatServerBuilder.newBuilder().serverHost(serverOneHost).serverPort(serverOnePort)
                .workerCount(serverOneWorkerCount)
                .build();
        serverOne.start();
        assertTrue(serverOne.isRunning());

        // 2. setup and start clientOne: clientOne is alive
        final HeartbeatClient clientOne = HeartbeatClient.getClient(serverOneHost, serverOnePort, serverOneDeadlineSeconds, clientOneWorkerCount);
        clientOne.start();
        assertTrue(clientOne.isRunning());

        // 3. setup and start serverTwo: serverTwo is alive
        final String serverTwoHost = "127.0.0.1";
        final int serverTwoPort = 8484;
        final long serverTwoDeadlineSeconds = 1L;
        final int serverTwoWorkerCount = 1;
        final int clientTwoWorkerCount = 1;

        final HeartbeatServer serverTwo = HeartbeatServerBuilder.newBuilder().serverHost(serverTwoHost).serverPort(serverTwoPort)
                .workerCount(serverTwoWorkerCount)
                .build();
        serverTwo.start();
        assertTrue(serverTwo.isRunning());

        // 4. setup and start clientTwo: clientTwo is alive
        final HeartbeatClient clientTwo = HeartbeatClient.getClient(serverTwoHost, serverTwoPort, serverTwoDeadlineSeconds, clientTwoWorkerCount);
        clientTwo.start();
        assertTrue(clientTwo.isRunning());

        // 5. call registerPeer(serverTwo) on clientOne of serverOne: serverOne->serverTwo heartbeating starts
        final RegisterPeerRequest registerPeerTwoWithServerOneRequest = RegisterPeerRequest.newBuilder().setPeerHost(serverTwoHost)
                .setPeerPort(serverTwoPort).setPeerId(serverTwo.getIdentity()).build();
        final RegisterPeerResponse registerPeerTwoWithServerOneResponse = clientOne.registerPeer(registerPeerTwoWithServerOneRequest);
        assertNotNull(registerPeerTwoWithServerOneResponse);

        // 6. call registerPeer(serverOne) on clientTwo of serverTwo: serverTwo->serverOne heartbeating starts
        final RegisterPeerRequest registerPeerOneWithServerTwoRequest = RegisterPeerRequest.newBuilder().setPeerHost(serverOneHost)
                .setPeerPort(serverOnePort).setPeerId(serverOne.getIdentity()).build();
        final RegisterPeerResponse registerPeerOneWithServerTwoResponse = clientTwo.registerPeer(registerPeerOneWithServerTwoRequest);
        assertNotNull(registerPeerOneWithServerTwoResponse);

        LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(500L, TimeUnit.MILLISECONDS));

        // 7. call deregisterPeer(serverTwo) on clientOne of serverOne: serverOne->serverTwo heartbeating stops
        final DeregisterPeerRequest deregisterPeerTwoWithServerOneRequest = DeregisterPeerRequest.newBuilder()
                .setPeerId(serverTwo.getIdentity()).build();
        final DeregisterPeerResponse deregisterPeerTwoWithServerOneResponse = clientOne.deregisterPeer(deregisterPeerTwoWithServerOneRequest);
        assertNotNull(deregisterPeerTwoWithServerOneResponse);

        // 8. call deregisterPeer(serverOne) on clientTwo of serverTwo: serverTwo->serverOne heartbeating stops
        final DeregisterPeerRequest deregisterPeerOneWithServerTwoRequest = DeregisterPeerRequest.newBuilder()
                .setPeerId(serverOne.getIdentity()).build();
        final DeregisterPeerResponse deregisterPeerOneWithServerTwoResponse = clientTwo.deregisterPeer(deregisterPeerOneWithServerTwoRequest);
        assertNotNull(deregisterPeerOneWithServerTwoResponse);

        // n. shut e'thing down
        if (clientOne != null) {
            clientOne.stop();
            assertFalse(clientOne.isRunning());
        }
        if (serverOne != null) {
            serverOne.stop();
            assertFalse(serverOne.isRunning());
        }
        if (clientTwo != null) {
            clientTwo.stop();
            assertFalse(clientTwo.isRunning());
        }
        if (serverTwo != null) {
            serverTwo.stop();
            assertFalse(serverTwo.isRunning());
        }
    }

}
