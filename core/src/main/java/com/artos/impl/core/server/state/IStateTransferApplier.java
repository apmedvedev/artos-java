package com.artos.impl.core.server.state;

import java.io.File;

public interface IStateTransferApplier {
    void applySnapshot(File stateFile, long logIndex, boolean compressed);

    void applyLog(File stateFile, boolean compressed);
}
