package com.github.heartbeater.server;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.heartbeater.lib.Lifecycle;
import com.github.heartbeater.server.HeartbeatServerException.Code;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;

/**
 * RPC Server for serving clients of heartbeater.
 */
public final class HeartbeatServer implements Lifecycle {
    private static final Logger logger = LogManager.getLogger(HeartbeatServer.class.getSimpleName());

    private final String identity = UUID.randomUUID().toString();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean ready = new AtomicBoolean(false);

    private final String serverHost;
    private final int serverPort;
    private final int workerCount;

    private Server server;
    private Thread serverThread;
    private ThreadPoolExecutor serverExecutor;

    private HeartbeatPersister persister;

    private HeartbeatServer(final String serverHost, final int serverPort, final int workerCount) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.workerCount = workerCount;
    }

    public final static class HeartbeatServerBuilder {
        private String serverHost;
        private int serverPort;
        private int workerCount;

        public static HeartbeatServerBuilder newBuilder() {
            return new HeartbeatServerBuilder();
        }

        public HeartbeatServerBuilder serverHost(final String serverHost) {
            this.serverHost = serverHost;
            return this;
        }

        public HeartbeatServerBuilder serverPort(final int serverPort) {
            this.serverPort = serverPort;
            return this;
        }

        public HeartbeatServerBuilder workerCount(final int workerCount) {
            this.workerCount = workerCount;
            return this;
        }

        public HeartbeatServer build() throws HeartbeatServerException {
            if (serverHost == null || serverPort == 0 || workerCount == 0) {
                throw new HeartbeatServerException(Code.HEARTBEATER_INIT_FAILURE,
                        "serverHost, serverPort, workerCount all need to be specified");
            }
            return new HeartbeatServer(serverHost, serverPort, workerCount);
        }

        private HeartbeatServerBuilder() {
        }
    }

    @Override
    public String getIdentity() {
        return identity;
    }

    @Override
    public void start() throws Exception {
        if (running.compareAndSet(false, true)) {
            final CountDownLatch serverReadyLatch = new CountDownLatch(1);
            serverThread = new Thread() {
                {
                    setName("heartbeat-server-main");
                    setDaemon(true);
                }

                @Override
                public void run() {
                    final long startMillis = System.currentTimeMillis();
                    logger.info("Starting HeartbeatServer [{}] at port {}", getIdentity(), serverPort);
                    try {
                        persister = HeartbeatPersister.getPersister();
                        persister.start();

                        serverExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(workerCount, new ThreadFactory() {
                            private final AtomicInteger threadIter = new AtomicInteger();
                            private final String threadNamePattern = "heartbeat-server-%d";

                            @Override
                            public Thread newThread(final Runnable runnable) {
                                final Thread worker = new Thread(runnable, String.format(threadNamePattern, threadIter.incrementAndGet()));
                                worker.setDaemon(true);
                                return worker;
                            }
                        });
                        final HeartbeatServiceImpl service = new HeartbeatServiceImpl(persister);
                        server = NettyServerBuilder.forAddress(new InetSocketAddress(serverHost, serverPort))
                                .addService(service).intercept(TransmitStatusRuntimeExceptionInterceptor.instance()).executor(serverExecutor).build();
                        server.start();
                        serverReadyLatch.countDown();
                        logger.info("Started HeartbeatServer [{}] at port {} in {} millis", getIdentity(), serverPort,
                                (System.currentTimeMillis() - startMillis));
                        server.awaitTermination();
                    } catch (Exception serverProblem) {
                        logger.error("Failed to start HeartbeatServer [{}] at port {} in {} millis", getIdentity(), serverPort,
                                (System.currentTimeMillis() - startMillis), serverProblem);
                    }
                }
            };
            serverThread.start();
            if (serverReadyLatch.await(2L, TimeUnit.SECONDS)) {
                ready.set(true);
            } else {
                logger.error("Failed to start HeartbeatServer [{}] at port", getIdentity(), serverPort);
            }
        } else {
            logger.error("Invalid attempt to start an already running HeartbeatServer");
        }
    }

    @Override
    public void stop() throws Exception {
        final long startMillis = System.currentTimeMillis();
        logger.info("Stopping HeartbeatServer [{}]", getIdentity());
        if (running.compareAndSet(true, false)) {
            ready.set(false);
            if (persister.isRunning()) {
                persister.stop();
            }
            if (!server.isTerminated()) {
                server.shutdown();
                server.awaitTermination(2L, TimeUnit.SECONDS);
                serverThread.interrupt();
                logger.info("Stopped heartbeat server main thread");
            }
            if (serverExecutor != null && !serverExecutor.isTerminated()) {
                serverExecutor.shutdown();
                serverExecutor.awaitTermination(2L, TimeUnit.SECONDS);
                serverExecutor.shutdownNow();
                logger.info("Stopped heartbeat server worker threads");
            }
            logger.info("Stopped HeartbeatServer [{}] in {} millis", getIdentity(), (System.currentTimeMillis() - startMillis));
        } else {
            logger.error("Invalid attempt to stop an already stopped HeartbeatServer");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get() && ready.get();
    }
}
