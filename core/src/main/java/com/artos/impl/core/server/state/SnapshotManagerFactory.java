package com.artos.impl.core.server.state;

import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.replication.IReplicationProtocol;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Files;

import java.io.File;

public class SnapshotManagerFactory implements ISnapshotManagerFactory {
    private final Context context;
    private IReplicationProtocol replicationProtocol;
    private SnapshotManager snapshotManager;
    private Snapshot snapshot;
    private File workDir;
    private boolean closed;
    private boolean snapshotLoaded;

    public SnapshotManagerFactory(Context context) {
        Assert.notNull(context);

        this.context = context;
    }

    public void setReplicationProtocol(IReplicationProtocol replicationProtocol) {
        Assert.notNull(replicationProtocol);
        Assert.isNull(this.replicationProtocol);

        this.replicationProtocol = replicationProtocol;
    }

    @Override
    public synchronized ISnapshotManager create() {
        Assert.checkState(!closed);
        Assert.checkState(snapshotManager == null);

        snapshotManager = new SnapshotManager(this, context, replicationProtocol, snapshot, snapshotLoaded, workDir);
        return snapshotManager;
    }

    synchronized void clear(SnapshotManager manager, Snapshot snapshot, boolean snapshotLoaded) {
        Assert.checkState(snapshotManager == manager);

        this.snapshot = snapshot;
        this.snapshotLoaded = snapshotLoaded;
        snapshotManager = null;
    }

    @Override
    public void start() {
        workDir = new File(context.getServerChannelConfiguration().getWorkDir(), "snapshots");
        if (!workDir.exists())
            workDir.mkdirs();
    }

    @Override
    public synchronized void stop() {
        if (snapshotManager != null)
            snapshotManager.close();

        closed = true;
    }

    public void uninstall() {
        stop();

        Files.emptyDir(workDir);
    }
}
