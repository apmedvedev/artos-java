package com.artos.impl.core.message;

public enum MessageFlags {
    REQUEST_CONFIGURATION,
    STATE_TRANSFER_REQUIRED,
    OUT_OF_ORDER_RECEIVED,
    RESTART_SESSION,
    CATCHING_UP,
    ALREADY_JOINED,
    REWIND_REPEAT_JOIN,
    TRANSFER_SNAPSHOT,
    TRANSFER_LOG,
    ALREADY_LEFT,
    PRE_VOTE
}
