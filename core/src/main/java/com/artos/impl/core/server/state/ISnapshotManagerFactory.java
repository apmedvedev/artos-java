package com.artos.impl.core.server.state;

import com.exametrika.common.utils.ILifecycle;

public interface ISnapshotManagerFactory extends ILifecycle {
    ISnapshotManager create();
}
