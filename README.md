# Heartbeater
The heartbeater is a module responsible for maintaining heartbeats between peer processes on the same or on remote machines. It provides a very simple api and can act as both the client as well as the server to send and receive heartbeats.

## Client API Reference
```java
    RegisterPeerResponse registerPeer(final RegisterPeerRequest registerPeerRequest) throws HeartbeatClientException;

    DeregisterPeerResponse deregisterPeer(final DeregisterPeerRequest deregisterPeerRequest) throws HeartbeatClientException;

    HeartbeatResponse heartbeat(final HeartbeatMessage heartbeatMessage) throws HeartbeatClientException;

    static HeartbeatClient getClient(final String serverHost, final int serverPort, final int serverDeadlineMillis, final int workerCount);
```

## Usage Example
```java
  HeartbeatServer serverOne = null;
  HeartbeatClient clientOne = null;
  HeartbeatServer serverTwo = null;
  HeartbeatClient clientTwo = null;
  try {
      // 1. setup and start serverOne; check that serverOne is alive
      final String serverOneHost = "127.0.0.1";
      final int serverOnePort = 8181;
      final int serverOneDeadlineMillis = 500;
      final int serverOneWorkerCount = 1;
      final int clientOneWorkerCount = 1;
      final int serverOneEpoch = 2;
      serverOne = HeartbeatServerBuilder.newBuilder().serverHost(serverOneHost).serverPort(serverOnePort)
              .workerCount(serverOneWorkerCount).serverEpoch(serverOneEpoch).build();
      serverOne.start();
      assertTrue(serverOne.isRunning());

      // 2. setup and start clientOne; check that clientOne is alive
      clientOne = HeartbeatClient.getClient(serverOneHost, serverOnePort, serverOneDeadlineMillis, clientOneWorkerCount);
      clientOne.start();
      assertTrue(clientOne.isRunning());

      // 3. setup and start serverTwo; check that serverTwo is alive
      final String serverTwoHost = "127.0.0.1";
      final int serverTwoPort = 8484;
      final int serverTwoDeadlineMillis = 500L;
      final int serverTwoWorkerCount = 1;
      final int clientTwoWorkerCount = 1;
      final int serverTwoEpoch = 3;
      serverTwo = HeartbeatServerBuilder.newBuilder().serverHost(serverTwoHost).serverPort(serverTwoPort)
              .workerCount(serverTwoWorkerCount).serverEpoch(serverTwoEpoch).build();
      serverTwo.start();
      assertTrue(serverTwo.isRunning());

      // 4. setup and start clientTwo; check that clientTwo is alive
      clientTwo = HeartbeatClient.getClient(serverTwoHost, serverTwoPort, serverTwoDeadlineMillis, clientTwoWorkerCount);
      clientTwo.start();
      assertTrue(clientTwo.isRunning());

      final int heartbeatFreqMillis = 5;

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

      // 8. call deregisterPeer(serverTwo) on clientOne of serverOne; check that serverOne->serverTwo heartbeating stops
      final DeregisterPeerRequest deregisterPeerTwoWithServerOneRequest = DeregisterPeerRequest.newBuilder()
              .setPeerId(serverTwo.getIdentity()).build();
      final DeregisterPeerResponse deregisterPeerTwoWithServerOneResponse = clientOne.deregisterPeer(deregisterPeerTwoWithServerOneRequest);
      assertNotNull(deregisterPeerTwoWithServerOneResponse);

      // 9. call deregisterPeer(serverOne) on clientTwo of serverTwo; check serverTwo->serverOne heartbeating stops
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
```

## Persistence
Heartbeater uses rocksdb as a local cache for persistence of its peer registry.
