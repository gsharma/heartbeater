package com.github.heartbeater.client;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.heartbeater.client.HeartbeatClientException.Code;
import com.github.heartbeater.rpc.HeartbeatMessage;
import com.github.heartbeater.rpc.HeartbeatResponse;
import com.github.heartbeater.rpc.HeartbeatServiceGrpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

/**
 * A simple heartbeater client implementation.
 */
public final class HeartbeatClientImpl implements HeartbeatClient {
    private static final Logger logger = LogManager.getLogger(HeartbeatClientImpl.class.getSimpleName());

    private final String identity = UUID.randomUUID().toString();
    private final AtomicLong clientEpoch;

    private final AtomicBoolean running;
    private final AtomicBoolean ready;

    private final String serverHost;
    private final int serverPort;
    private final long serverDeadlineSeconds;
    private final int workerCount;

    private ManagedChannel channel;
    private HeartbeatServiceGrpc.HeartbeatServiceBlockingStub serviceStub;
    private ThreadPoolExecutor clientExecutor;

    HeartbeatClientImpl(final String serverHost, final int serverPort, final long serverDeadlineSeconds, final int workerCount) {
        this.running = new AtomicBoolean(false);
        this.ready = new AtomicBoolean(false);
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.serverDeadlineSeconds = serverDeadlineSeconds;
        this.workerCount = workerCount;
        this.clientEpoch = new AtomicLong();
    }

    @Override
    public void start() throws HeartbeatClientException {
        if (running.compareAndSet(false, true)) {
            clientExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(workerCount, new ThreadFactory() {
                private final AtomicInteger threadIter = new AtomicInteger();
                private final String threadNamePattern = "heartbeat-client-%d";

                @Override
                public Thread newThread(final Runnable runnable) {
                    return new Thread(runnable, String.format(threadNamePattern, threadIter.incrementAndGet()));
                }
            });
            final ClientInterceptor deadlineInterceptor = new ClientInterceptor() {
                @Override
                public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                        final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions, final Channel next) {
                    logger.debug("Intercepted {}", method.getFullMethodName());
                    return next.newCall(method, callOptions.withDeadlineAfter(serverDeadlineSeconds, TimeUnit.SECONDS));
                }
            };
            channel = ManagedChannelBuilder.forAddress(serverHost, serverPort).usePlaintext()
                    .executor(clientExecutor).offloadExecutor(clientExecutor)
                    .intercept(deadlineInterceptor)
                    .userAgent("heartbeat-client").build();
            serviceStub = HeartbeatServiceGrpc.newBlockingStub(channel).withWaitForReady();
            ready.set(true);
            logger.info("Started HeartbeatClient [{}]", getIdentity());
        }
    }

    @Override
    public void stop() throws HeartbeatClientException {
        if (running.compareAndSet(true, false)) {
            try {
                ready.set(false);
                channel.shutdownNow().awaitTermination(1L, TimeUnit.SECONDS);
                channel.shutdown();
                if (clientExecutor != null && !clientExecutor.isTerminated()) {
                    clientExecutor.shutdown();
                    clientExecutor.awaitTermination(2L, TimeUnit.SECONDS);
                    clientExecutor.shutdownNow();
                    logger.info("Stopped heartbeat client worker threads");
                }
                logger.info("Stopped HeartbeatClient [{}]", getIdentity());
            } catch (Exception tiniProblem) {
                logger.error(tiniProblem);
            }
        }
    }

    @Override
    public String getIdentity() {
        return identity;
    }

    @Override
    public boolean isRunning() {
        return running.get() && ready.get();
    }

    @Override
    public HeartbeatResponse heartbeat(HeartbeatMessage heartbeatMessage) throws HeartbeatClientException {
        if (!isRunning()) {
            throw new HeartbeatClientException(Code.INVALID_HEARTBEATER_CLIENT_LCM, "Invalid attempt to operate an already stopped heartbeat client");
        }
        HeartbeatResponse response = null;
        try {
            response = serviceStub.heartbeat(heartbeatMessage);
        } catch (Throwable problem) {
            toHeartbeatClientException(problem);
        }
        return response;
    }

    private static void toHeartbeatClientException(final Throwable problem) throws HeartbeatClientException {
        if (problem instanceof StatusException) {
            final StatusException statusException = StatusException.class.cast(problem);
            final String status = statusException.getStatus().toString();
            throw new HeartbeatClientException(Code.HEARTBEATER_SERVER_ERROR, status, statusException);
        } else if (problem instanceof StatusRuntimeException) {
            final StatusRuntimeException statusRuntimeException = StatusRuntimeException.class.cast(problem);
            final String status = statusRuntimeException.getStatus().toString();
            throw new HeartbeatClientException(Code.HEARTBEATER_SERVER_ERROR, status, statusRuntimeException);
        } else if (problem instanceof HeartbeatClientException) {
            throw HeartbeatClientException.class.cast(problem);
        } else {
            throw new HeartbeatClientException(Code.UNKNOWN_FAILURE, problem);
        }
    }

}
