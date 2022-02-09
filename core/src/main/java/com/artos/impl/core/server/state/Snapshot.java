package com.artos.impl.core.server.state;

import com.exametrika.common.utils.Assert;

import java.io.File;

public class Snapshot {
    private final File file;
    private final long logIndex;
    private final long creationTime;

    public Snapshot(File file, long logIndex, long creationTime) {
        Assert.notNull(file);

        this.file = file;
        this.logIndex = logIndex;
        this.creationTime = creationTime;
    }

    public File getFile() {
        return file;
    }

    public long getLogIndex() {
        return logIndex;
    }

    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public String toString() {
        return file.toString() + ":" + logIndex;
    }
}
