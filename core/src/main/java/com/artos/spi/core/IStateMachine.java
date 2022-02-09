/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.spi.core;

import com.exametrika.common.tasks.IFlowController;
import com.exametrika.common.utils.ILifecycle;

import java.io.InputStream;
import java.io.OutputStream;

public interface IStateMachine extends IFlowController<FlowType>, ILifecycle {

    IStateMachineTransaction beginTransaction(boolean readOnly);

    /**
     * Acquires snapshot of state machine. Application data and configuration are included in snapshot, server state is not included.
     *
     * @param stream stream to write snapshot to
     * @return last committed log index, applied to state machine
     */
    long acquireSnapshot(OutputStream stream);

    /**
     * Applies snapshot of state machine. Application data and configuration are included in snapshot, server state is not included.
     *
     * @param logIndex log index for given snapshot
     * @param stream stream to read snapshot from
     */
    void applySnapshot(long logIndex, InputStream stream);
}
