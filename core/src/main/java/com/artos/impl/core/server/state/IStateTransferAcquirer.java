package com.artos.impl.core.server.state;

import java.io.File;
import java.util.List;
import java.util.UUID;

public interface IStateTransferAcquirer {
    class LogState {
        public File file;
        public boolean compressed;
    }

    class SnapshotState extends LogState {
        public long logIndex;
    }

    class StateInfo {
        public SnapshotState snapshotState;
        public List<LogState> logStates;
    }

    StateInfo acquireState(UUID groupId, long logIndex, File workDir);
}
