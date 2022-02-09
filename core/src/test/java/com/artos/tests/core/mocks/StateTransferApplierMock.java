package com.artos.tests.core.mocks;

import com.artos.impl.core.server.state.IStateTransferApplier;
import com.exametrika.common.io.impl.ByteInputStream;
import com.exametrika.common.io.impl.ByteOutputStream;
import com.exametrika.common.io.impl.DataDeserialization;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.Exceptions;
import com.exametrika.common.utils.IOs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class StateTransferApplierMock implements IStateTransferApplier {
    private long logIndex;
    private boolean compressed;
    private ByteArray snapshot;
    private List<ByteArray> logs = new ArrayList<>();

    @Override
    public void applySnapshot(File stateFile, long logIndex, boolean compressed) {
        Assert.checkState(this.logIndex == logIndex);
        Assert.checkState(this.compressed == compressed);

        snapshot = read(stateFile);
    }

    @Override
    public void applyLog(File stateFile, boolean compressed) {
        Assert.checkState(this.compressed == compressed);

        logs.add(read(stateFile));
    }

    private ByteArray read(File file) {
        ByteOutputStream out = new ByteOutputStream();

        try (InputStream stream = new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            IOs.copy(stream, out);
        } catch (Exception e) {
            Exceptions.wrapAndThrow(e);
        }

        ByteInputStream in = new ByteInputStream(out.getBuffer(), 0, out.getLength());
        DataDeserialization deserialization = new DataDeserialization(in);
        return deserialization.readByteArray();
    }

    public void setLogIndex(long logIndex) {
        this.logIndex = logIndex;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }

    public ByteArray getSnapshot() {
        return snapshot;
    }

    public List<ByteArray> getLogs() {
        return logs;
    }
}
