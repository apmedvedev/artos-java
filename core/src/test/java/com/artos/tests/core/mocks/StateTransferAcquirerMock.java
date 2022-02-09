package com.artos.tests.core.mocks;

import com.artos.impl.core.server.state.IStateTransferAcquirer;
import com.exametrika.common.io.impl.ByteInputStream;
import com.exametrika.common.io.impl.ByteOutputStream;
import com.exametrika.common.io.impl.DataSerialization;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.Exceptions;
import com.exametrika.common.utils.Files;
import com.exametrika.common.utils.IOs;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

public class StateTransferAcquirerMock implements IStateTransferAcquirer {
    private UUID groupId;
    private long logIndex;
    private ByteArray snapshot;
    private List<ByteArray> logs;

    @Override
    public StateInfo acquireState(UUID groupId, long logIndex, File workDir) {
        Assert.checkState(this.groupId.equals(groupId));
        Assert.checkState(this.logIndex == logIndex);

        StateInfo stateInfo = new StateInfo();
        File snapshotFile = Files.createTempFile("raft", "tmp", workDir);
        write(snapshotFile, snapshot);

        SnapshotState snapshotState = new SnapshotState();
        snapshotState.file = snapshotFile;
        snapshotState.compressed = true;
        snapshotState.logIndex = logIndex;
        stateInfo.snapshotState = snapshotState;

        stateInfo.logStates = new ArrayList<>();
        for (ByteArray log : logs) {
            File logFile = Files.createTempFile("raft", "tmp", workDir);
            write(logFile, log);

            LogState logState = new LogState();
            logState.file = logFile;
            logState.compressed = true;
            stateInfo.logStates.add(logState);
        }

        return stateInfo;
    }

    public UUID getGroupId() {
        return groupId;
    }

    public void setGroupId(UUID groupId) {
        this.groupId = groupId;
    }

    public long getLogIndex() {
        return logIndex;
    }

    public void setLogIndex(long logIndex) {
        this.logIndex = logIndex;
    }

    public void setSnapshot(ByteArray snapshot) {
        this.snapshot = snapshot;
    }

    public void setLogs(List<ByteArray> logs) {
        this.logs = logs;
    }

    private void write(File file, ByteArray data) {
        ByteOutputStream out = new ByteOutputStream();
        DataSerialization serialization = new DataSerialization(out);
        serialization.writeByteArray(data);
        ByteInputStream in = new ByteInputStream(out.getBuffer(), 0, out.getLength());

        try (OutputStream stream = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
            IOs.copy(in, stream);
        } catch (Exception e) {
            Exceptions.wrapAndThrow(e);
        }
    }
}
