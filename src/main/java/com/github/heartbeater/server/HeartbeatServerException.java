package com.github.heartbeater.server;

/**
 * Unified single exception that's thrown and handled by various server-side components of Heartbeater. The idea is to use the code enum to
 * encapsulate various error/exception conditions. That said, stack traces, where available and desired, are not meant to be kept from users.
 */
public final class HeartbeatServerException extends Exception {
    private static final long serialVersionUID = 1L;
    private final Code code;

    public HeartbeatServerException(final Code code) {
        super(code.getDescription());
        this.code = code;
    }

    public HeartbeatServerException(final Code code, final String message) {
        super(message);
        this.code = code;
    }

    public HeartbeatServerException(final Code code, final Throwable throwable) {
        super(throwable);
        this.code = code;
    }

    public Code getCode() {
        return code;
    }

    public static enum Code {
        // 1.
        INVALID_HEARTBEATER_LCM("Heartbeater cannot retransition to the same desired state"),
        // 2.
        HEARTBEATER_INIT_FAILURE("Failed to initialize the heartbeater"),
        // 3.
        HEARTBEATER_PERSISTENCE_FAILURE("Issue encountered with heartbeater persistence"),
        // 4.
        HEARTBEATER_TINI_FAILURE("Failed to cleanly shutdown the heartbeater"),
        // 5.
        HEARTBEATER_INVALID_ARG("Invalid arguments passed"),
        // 6.
        PEER_ALREADY_EXISTS("Peer already exists"),
        // n.
        UNKNOWN_FAILURE(
                "Heartbeater internal failure. Check exception stacktrace for more details of the failure");

        private String description;

        private Code(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }
}
