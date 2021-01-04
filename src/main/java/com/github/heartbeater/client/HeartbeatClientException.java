package com.github.heartbeater.client;

/**
 * Unified single exception that's thrown and handled by various client-side components of Heartbeater. The idea is to use the code enum to
 * encapsulate various error/exception conditions. That said, stack traces, where available and desired, are not meant to be kept from users.
 */
public final class HeartbeatClientException extends Exception {
    private static final long serialVersionUID = 1L;
    private final Code code;

    public HeartbeatClientException(final Code code) {
        super(code.getDescription());
        this.code = code;
    }

    public HeartbeatClientException(final Code code, final String message) {
        super(message);
        this.code = code;
    }

    public HeartbeatClientException(final Code code, final Throwable throwable) {
        super(throwable);
        this.code = code;
    }

    public HeartbeatClientException(final Code code, final String message, final Throwable throwable) {
        super(message, throwable);
        this.code = code;
    }

    public Code getCode() {
        return code;
    }

    public static enum Code {
        // 1.
        INVALID_HEARTBEATER_CLIENT_LCM("Heartbeater client cannot retransition to the same desired state"),
        // 2.
        HEARTBEATER_CLIENT_INIT_FAILURE("Failed to initialize the heartbeater client"),
        // 3.
        HEARTBEATER_CLIENT_TINI_FAILURE("Failed to cleanly shutdown the heartbeater client"),
        // 4.
        HEARTBEATER_INVALID_ARG("Invalid arguments passed"),
        // 5.
        HEARTBEATER_SERVER_DEADLINE_EXCEEDED("Heartbeater server failed to respond within its deadline"),
        // 6.
        HEARTBEATER_SERVER_ERROR("Heartbeater server encountered an error"),
        // n.
        UNKNOWN_FAILURE(
                "Heartbeater client internal failure. Check exception stacktrace for more details of the failure");

        private String description;

        private Code(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }
}
