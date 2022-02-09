package com.artos.impl.core.server.state;

public interface ISnapshotManager extends AutoCloseable {
    Snapshot getSnapshot();

    Snapshot createSnapshot();

    void removeSnapshot();
}
