package com.github.heartbeater;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import com.github.heartbeater.client.HeartbeatClient;
import com.github.heartbeater.rpc.DeregisterPeerRequest;
import com.github.heartbeater.rpc.DeregisterPeerResponse;
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

    @Test
    public void test2ServerHeartbeating() throws Exception {
        HeartbeatServer serverOne = null;
        HeartbeatClient clientOne = null;
        HeartbeatServer serverTwo = null;
        HeartbeatClient clientTwo = null;
        try {
            // 1. setup and start serverOne: serverOne is alive
            final String serverOneHost = "127.0.0.1";
            final int serverOnePort = 8181;
            final int heartbeatFreqMillis = 5;
            final int serverOneDeadlineMillis = heartbeatFreqMillis * 100;
            final int serverOneWorkerCount = 1;
            final int clientOneWorkerCount = 1;
            final int serverOneEpoch = 2;

            serverOne = HeartbeatServerBuilder.newBuilder().serverHost(serverOneHost).serverPort(serverOnePort)
                    .workerCount(serverOneWorkerCount).serverEpoch(serverOneEpoch).build();
            serverOne.start();
            assertTrue(serverOne.isRunning());

            // 2. setup and start clientOne: clientOne is alive
            clientOne = HeartbeatClient.getClient(serverOneHost, serverOnePort, serverOneDeadlineMillis, clientOneWorkerCount);
            clientOne.start();
            assertTrue(clientOne.isRunning());

            // 3. setup and start serverTwo: serverTwo is alive
            final String serverTwoHost = "127.0.0.1";
            final int serverTwoPort = 8484;
            final int serverTwoDeadlineMillis = heartbeatFreqMillis * 100;
            final int serverTwoWorkerCount = 1;
            final int clientTwoWorkerCount = 1;
            final int serverTwoEpoch = 3;

            serverTwo = HeartbeatServerBuilder.newBuilder().serverHost(serverTwoHost).serverPort(serverTwoPort)
                    .workerCount(serverTwoWorkerCount).serverEpoch(serverTwoEpoch).build();
            serverTwo.start();
            assertTrue(serverTwo.isRunning());

            // 4. setup and start clientTwo: clientTwo is alive
            clientTwo = HeartbeatClient.getClient(serverTwoHost, serverTwoPort, serverTwoDeadlineMillis, clientTwoWorkerCount);
            clientTwo.start();
            assertTrue(clientTwo.isRunning());

            // 5. call registerPeer(serverTwo) on clientOne of serverOne: serverOne->serverTwo heartbeating starts
            final RegisterPeerRequest registerPeerTwoWithServerOneRequest = RegisterPeerRequest.newBuilder().setPeerHost(serverTwoHost)
                    .setPeerPort(serverTwoPort).setPeerId(serverTwo.getIdentity()).setHeartbeatFreqMillis(heartbeatFreqMillis).build();
            final RegisterPeerResponse registerPeerTwoWithServerOneResponse = clientOne.registerPeer(registerPeerTwoWithServerOneRequest);
            assertNotNull(registerPeerTwoWithServerOneResponse);

            // 6. call registerPeer(serverOne) on clientTwo of serverTwo: serverTwo->serverOne heartbeating starts
            final RegisterPeerRequest registerPeerOneWithServerTwoRequest = RegisterPeerRequest.newBuilder().setPeerHost(serverOneHost)
                    .setPeerPort(serverOnePort).setPeerId(serverOne.getIdentity()).setHeartbeatFreqMillis(heartbeatFreqMillis).build();
            final RegisterPeerResponse registerPeerOneWithServerTwoResponse = clientTwo.registerPeer(registerPeerOneWithServerTwoRequest);
            assertNotNull(registerPeerOneWithServerTwoResponse);

            // 7. wait for a few heartbeats to go through
            LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(heartbeatFreqMillis * 20, TimeUnit.MILLISECONDS));

            // 8. call deregisterPeer(serverTwo) on clientOne of serverOne: serverOne->serverTwo heartbeating stops
            final DeregisterPeerRequest deregisterPeerTwoWithServerOneRequest = DeregisterPeerRequest.newBuilder()
                    .setPeerId(serverTwo.getIdentity()).build();
            final DeregisterPeerResponse deregisterPeerTwoWithServerOneResponse = clientOne.deregisterPeer(deregisterPeerTwoWithServerOneRequest);
            assertNotNull(deregisterPeerTwoWithServerOneResponse);

            // 9. call deregisterPeer(serverOne) on clientTwo of serverTwo: serverTwo->serverOne heartbeating stops
            final DeregisterPeerRequest deregisterPeerOneWithServerTwoRequest = DeregisterPeerRequest.newBuilder()
                    .setPeerId(serverOne.getIdentity()).build();
            final DeregisterPeerResponse deregisterPeerOneWithServerTwoResponse = clientTwo.deregisterPeer(deregisterPeerOneWithServerTwoRequest);
            assertNotNull(deregisterPeerOneWithServerTwoResponse);
        } finally {
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

    @Test
    public void testLeaderFollowerHeartbeats() throws Exception {
        HeartbeatServer leaderServer = null;
        HeartbeatServer followerServerOne = null;
        HeartbeatServer followerServerTwo = null;
        HeartbeatServer followerServerThree = null;
        HeartbeatClient clientToLeader = null;
        try {
            // 1. setup and start leaderServer: leaderServer is alive
            final String leaderServerHost = "127.0.0.1";
            final int leaderServerPort = 8181;
            final int heartbeatFreqMillis = 5;
            final int leaderServerDeadlineMillis = heartbeatFreqMillis * 100;
            final int leaderServerWorkerCount = 1;
            final int leaderServerEpoch = 5;

            leaderServer = HeartbeatServerBuilder.newBuilder().serverHost(leaderServerHost).serverPort(leaderServerPort)
                    .workerCount(leaderServerWorkerCount).serverEpoch(leaderServerEpoch).build();
            leaderServer.start();
            assertTrue(leaderServer.isRunning());

            // 2. setup and start followerServerOne: followerServerOne is alive
            final String followerServerOneHost = "127.0.0.1";
            final int followerServerOnePort = 7001;
            final int followerServerOneWorkerCount = 1;
            final int followerServerOneEpoch = 1;

            followerServerOne = HeartbeatServerBuilder.newBuilder().serverHost(followerServerOneHost).serverPort(followerServerOnePort)
                    .workerCount(followerServerOneWorkerCount).serverEpoch(followerServerOneEpoch).build();
            followerServerOne.start();
            assertTrue(followerServerOne.isRunning());

            // 3. setup and start followerServerTwo: followerServerTwo is alive
            final String followerServerTwoHost = "127.0.0.1";
            final int followerServerTwoPort = 7002;
            final int followerServerTwoWorkerCount = 1;
            final int followerServerTwoEpoch = 2;

            followerServerTwo = HeartbeatServerBuilder.newBuilder().serverHost(followerServerTwoHost).serverPort(followerServerTwoPort)
                    .workerCount(followerServerTwoWorkerCount).serverEpoch(followerServerTwoEpoch).build();
            followerServerTwo.start();
            assertTrue(followerServerTwo.isRunning());

            // 4. setup and start followerServerThree: followerServerThree is alive
            final String followerServerThreeHost = "127.0.0.1";
            final int followerServerThreePort = 7003;
            final int followerServerThreeWorkerCount = 1;
            final int followerServerThreeEpoch = 3;

            followerServerThree = HeartbeatServerBuilder.newBuilder().serverHost(followerServerThreeHost).serverPort(followerServerThreePort)
                    .workerCount(followerServerThreeWorkerCount).serverEpoch(followerServerThreeEpoch).build();
            followerServerThree.start();
            assertTrue(followerServerThree.isRunning());

            // 5. setup and start clientToLeader: clientToLeader is alive
            clientToLeader = HeartbeatClient.getClient(leaderServerHost, leaderServerPort, leaderServerDeadlineMillis, 1);
            clientToLeader.start();
            assertTrue(clientToLeader.isRunning());

            // 6. call registerPeer(followerServerOne) on clientToLeader of leaderServer: leaderServer->followerServerOne heartbeating starts
            final RegisterPeerRequest registerFollowerOneWithLeaderRequest = RegisterPeerRequest.newBuilder().setPeerHost(followerServerOneHost)
                    .setPeerPort(followerServerOnePort).setPeerId(followerServerOne.getIdentity()).setHeartbeatFreqMillis(heartbeatFreqMillis)
                    .build();
            final RegisterPeerResponse registerFollowerOneWithLeaderResponse = clientToLeader.registerPeer(registerFollowerOneWithLeaderRequest);
            assertNotNull(registerFollowerOneWithLeaderResponse);

            // 7. call registerPeer(followerServerTwo) on clientToLeader of leaderServer: leaderServer->followerServerTwo heartbeating starts
            final RegisterPeerRequest registerFollowerTwoWithLeaderRequest = RegisterPeerRequest.newBuilder().setPeerHost(followerServerTwoHost)
                    .setPeerPort(followerServerTwoPort).setPeerId(followerServerTwo.getIdentity()).setHeartbeatFreqMillis(heartbeatFreqMillis)
                    .build();
            final RegisterPeerResponse registerFollowerTwoWithLeaderResponse = clientToLeader.registerPeer(registerFollowerTwoWithLeaderRequest);
            assertNotNull(registerFollowerTwoWithLeaderResponse);

            // 8. call registerPeer(followerServerThree) on clientToLeader of leaderServer: leaderServer->followerServerThree heartbeating starts
            final RegisterPeerRequest registerFollowerThreeWithLeaderRequest = RegisterPeerRequest.newBuilder().setPeerHost(followerServerThreeHost)
                    .setPeerPort(followerServerThreePort).setPeerId(followerServerThree.getIdentity()).setHeartbeatFreqMillis(heartbeatFreqMillis)
                    .build();
            final RegisterPeerResponse registerFollowerThreeWithLeaderResponse = clientToLeader.registerPeer(registerFollowerThreeWithLeaderRequest);
            assertNotNull(registerFollowerThreeWithLeaderResponse);

            // 9. wait for a few heartbeats to go through
            LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(heartbeatFreqMillis * 20, TimeUnit.MILLISECONDS));

            // 10. call deregisterPeer(followerServerOne) on clientToLeader of leaderServer: leaderServer->followerServerOne heartbeating stops
            final DeregisterPeerRequest deregisterFollowerOneWithLeaderRequest = DeregisterPeerRequest.newBuilder()
                    .setPeerId(followerServerOne.getIdentity()).build();
            final DeregisterPeerResponse deregisterFollowerOneWithLeaderResponse = clientToLeader
                    .deregisterPeer(deregisterFollowerOneWithLeaderRequest);
            assertNotNull(deregisterFollowerOneWithLeaderResponse);

            // 11. call deregisterPeer(followerServerTwo) on clientToLeader of leaderServer: leaderServer->followerServerTwo heartbeating stops
            final DeregisterPeerRequest deregisterFollowerTwoWithLeaderRequest = DeregisterPeerRequest.newBuilder()
                    .setPeerId(followerServerTwo.getIdentity()).build();
            final DeregisterPeerResponse deregisterFollowerTwoWithLeaderResponse = clientToLeader
                    .deregisterPeer(deregisterFollowerTwoWithLeaderRequest);
            assertNotNull(deregisterFollowerTwoWithLeaderResponse);

            // 12. call deregisterPeer(followerServerThree) on clientToLeader of leaderServer: leaderServer->followerServerThree heartbeating stops
            final DeregisterPeerRequest deregisterFollowerThreeWithLeaderRequest = DeregisterPeerRequest.newBuilder()
                    .setPeerId(followerServerThree.getIdentity()).build();
            final DeregisterPeerResponse deregisterFollowerThreeWithLeaderResponse = clientToLeader
                    .deregisterPeer(deregisterFollowerThreeWithLeaderRequest);
            assertNotNull(deregisterFollowerThreeWithLeaderResponse);
        } finally {
            // n. shut e'thing down
            if (clientToLeader != null) {
                clientToLeader.stop();
                assertFalse(clientToLeader.isRunning());
            }
            if (leaderServer != null) {
                leaderServer.stop();
                assertFalse(leaderServer.isRunning());
            }
            if (followerServerOne != null) {
                followerServerOne.stop();
                assertFalse(followerServerOne.isRunning());
            }
            if (followerServerTwo != null) {
                followerServerTwo.stop();
                assertFalse(followerServerTwo.isRunning());
            }
            if (followerServerThree != null) {
                followerServerThree.stop();
                assertFalse(followerServerThree.isRunning());
            }
        }
    }

    @Test
    public void testUnresponsivePeer() throws Exception {
        System.setProperty("heartbeatservice.expireheartbeatdeadline", "true");
        HeartbeatServer leaderServer = null;
        HeartbeatServer unresponsivePeer = null;
        HeartbeatClient clientToLeader = null;
        try {
            // 1. setup and start leaderServer: leaderServer is alive
            final String leaderServerHost = "127.0.0.1";
            final int leaderServerPort = 8181;
            final int heartbeatFreqMillis = 10;
            final int leaderServerDeadlineMillis = heartbeatFreqMillis * 100;
            final int leaderServerWorkerCount = 1;
            final int leaderServerEpoch = 5;

            leaderServer = HeartbeatServerBuilder.newBuilder().serverHost(leaderServerHost).serverPort(leaderServerPort)
                    .workerCount(leaderServerWorkerCount).serverEpoch(leaderServerEpoch).build();
            leaderServer.start();
            assertTrue(leaderServer.isRunning());

            // 2. setup and start unresponsivePeer: unresponsivePeer is alive
            final String unresponsivePeerHost = "127.0.0.1";
            final int unresponsivePeerPort = 7001;
            final int unresponsivePeerWorkerCount = 1;
            final int unresponsivePeerEpoch = 1;

            unresponsivePeer = HeartbeatServerBuilder.newBuilder().serverHost(unresponsivePeerHost).serverPort(unresponsivePeerPort)
                    .workerCount(unresponsivePeerWorkerCount).serverEpoch(unresponsivePeerEpoch).build();
            unresponsivePeer.start();
            assertTrue(unresponsivePeer.isRunning());

            // 3. setup and start clientToLeader: clientToLeader is alive
            clientToLeader = HeartbeatClient.getClient(leaderServerHost, leaderServerPort, leaderServerDeadlineMillis, 1);
            clientToLeader.start();
            assertTrue(clientToLeader.isRunning());

            // 4. call registerPeer(unresponsivePeer) on clientToLeader of leaderServer: leaderServer->unresponsivePeer heartbeating starts
            final RegisterPeerRequest registerUnresponsivePeerWithLeaderRequest = RegisterPeerRequest.newBuilder().setPeerHost(unresponsivePeerHost)
                    .setPeerPort(unresponsivePeerPort).setPeerId(unresponsivePeer.getIdentity()).setHeartbeatFreqMillis(heartbeatFreqMillis)
                    .build();
            final RegisterPeerResponse registerUnresponsivePeerWithLeaderResponse = clientToLeader
                    .registerPeer(registerUnresponsivePeerWithLeaderRequest);
            assertNotNull(registerUnresponsivePeerWithLeaderResponse);

            // 5. wait for a few heartbeats to go through
            LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(heartbeatFreqMillis * 10, TimeUnit.MILLISECONDS));

            // 6. call deregisterPeer(unresponsivePeer) on clientToLeader of leaderServer: leaderServer->unresponsivePeer heartbeating stops
            final DeregisterPeerRequest deregisterUnresponsivePeerWithLeaderRequest = DeregisterPeerRequest.newBuilder()
                    .setPeerId(unresponsivePeer.getIdentity()).build();
            final DeregisterPeerResponse deregisterUnresponsivePeerWithLeaderResponse = clientToLeader
                    .deregisterPeer(deregisterUnresponsivePeerWithLeaderRequest);
            assertNotNull(deregisterUnresponsivePeerWithLeaderResponse);
        } finally {
            System.setProperty("heartbeatservice.expireheartbeatdeadline", "false");
            // n. shut e'thing down
            if (clientToLeader != null) {
                clientToLeader.stop();
                assertFalse(clientToLeader.isRunning());
            }
            if (leaderServer != null) {
                leaderServer.stop();
                assertFalse(leaderServer.isRunning());
            }
            if (unresponsivePeer != null) {
                unresponsivePeer.stop();
                assertFalse(unresponsivePeer.isRunning());
            }
        }
    }

}
