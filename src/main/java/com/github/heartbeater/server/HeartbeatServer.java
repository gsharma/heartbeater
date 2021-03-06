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
    private final int serverEpoch;

    private Server server;
    private Thread serverThread;
    private ThreadPoolExecutor serverExecutor;

    private HeartbeatServiceImpl service;

    private HeartbeatServer(final String serverHost, final int serverPort, final int workerCount, final int serverEpoch) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.workerCount = workerCount;
        this.serverEpoch = serverEpoch;
    }

    public final static class HeartbeatServerBuilder {
        private String serverHost;
        private int serverPort = Integer.MIN_VALUE;
        private int workerCount = Integer.MIN_VALUE;
        private int serverEpoch = Integer.MIN_VALUE;

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

        public HeartbeatServerBuilder serverEpoch(final int serverEpoch) {
            this.serverEpoch = serverEpoch;
            return this;
        }

        public HeartbeatServer build() throws HeartbeatServerException {
            if (serverHost == null || serverHost.trim().isEmpty() || serverPort == Integer.MIN_VALUE || workerCount == Integer.MIN_VALUE
                    || serverEpoch == Integer.MIN_VALUE) {
                throw new HeartbeatServerException(Code.HEARTBEATER_INIT_FAILURE,
                        "serverHost, serverPort, workerCount, serverEpoch all need to be specified");
            }
            return new HeartbeatServer(serverHost, serverPort, workerCount, serverEpoch);
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
                    final long startNanos = System.nanoTime();
                    logger.debug("Starting HeartbeatServer [{}] at port {}", getIdentity(), serverPort);
                    try {
                        service = new HeartbeatServiceImpl(identity, serverEpoch);
                        service.start();

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

                        server = NettyServerBuilder.forAddress(new InetSocketAddress(serverHost, serverPort))
                                .addService(service).intercept(TransmitStatusRuntimeExceptionInterceptor.instance()).executor(serverExecutor).build();
                        server.start();
                        serverReadyLatch.countDown();
                        logger.info("Started HeartbeatServer [{}] at port {} in {} millis", getIdentity(), serverPort,
                                TimeUnit.MILLISECONDS.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS));
                        server.awaitTermination();
                    } catch (Exception serverProblem) {
                        logger.error("Failed to start HeartbeatServer [{}] at port {} in {} millis", getIdentity(), serverPort,
                                TimeUnit.MILLISECONDS.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS), serverProblem);
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
        final long startNanos = System.nanoTime();
        logger.info("Stopping HeartbeatServer [{}]", getIdentity());
        if (running.compareAndSet(true, false)) {
            ready.set(false);
            // first stop the listeners
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
            if (service.isRunning()) {
                service.stop();
            }
            logger.info("Stopped HeartbeatServer [{}] in {} millis", getIdentity(),
                    TimeUnit.MILLISECONDS.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS));
        } else {
            logger.error("Invalid attempt to stop an already stopped HeartbeatServer");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get() && ready.get();
    }
}
