package com.artos.impl.core.server.state;

import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.replication.ClientSession;
import com.artos.impl.core.server.replication.IReplicationProtocol;
import com.exametrika.common.compartment.ICompartmentTask;
import com.exametrika.common.io.impl.ByteOutputStream;
import com.exametrika.common.io.impl.DataSerialization;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Exceptions;
import com.exametrika.common.utils.Files;
import com.exametrika.common.utils.Pair;
import com.exametrika.common.utils.Serializers;
import com.exametrika.common.utils.SyncCompletionHandler;
import com.exametrika.common.utils.Times;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

public class SnapshotManager implements ISnapshotManager {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final SnapshotManagerFactory factory;
    private final Context context;
    private final IReplicationProtocol replicationProtocol;
    private final File workDir;
    private Snapshot snapshot;
    private boolean snapshotLoaded;
    private boolean closed;
    private SyncCompletionHandler completionHandler;

    public SnapshotManager(SnapshotManagerFactory factory, Context context, IReplicationProtocol replicationProtocol,
                           Snapshot snapshot, boolean snapshotLoaded, File workDir) {
        Assert.notNull(factory);
        Assert.notNull(context);
        Assert.notNull(replicationProtocol);
        Assert.notNull(workDir);

        this.factory = factory;
        this.context = context;
        this.replicationProtocol = replicationProtocol;
        this.workDir = workDir;
        this.snapshot = snapshot;
        this.snapshotLoaded = snapshotLoaded;
        this.marker = context.getMarker();
    }

    @Override
    public synchronized Snapshot getSnapshot() {
        loadSnapshot();

        synchronized (factory) {
            Assert.checkState(!closed);

            checkSnapshot();
            return snapshot;
        }
    }

    @Override
    public synchronized Snapshot createSnapshot() {
        loadSnapshot();

        synchronized (factory) {
            Assert.checkState(!closed);

            checkSnapshot();
            Assert.checkState(snapshot == null);
            Assert.checkState(completionHandler == null);

            completionHandler = new SyncCompletionHandler();
        }

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.snapshotCreationStarted());

        context.getCompartment().offer(() -> {
            replicationProtocol.lockCommits();

            List<Pair<UUID, Long>> clientSessions = new ArrayList<>();
            for (ClientSession session : replicationProtocol.getClientSessionManager().getSessions().values())
                clientSessions.add(new Pair<>(session.getClientId(), session.getLastCommittedMessageId()));

            context.getCompartment().execute(new ICompartmentTask() {
                @Override
                public Object execute() {
                    File file = Files.createTempFile("snapshot", "tmp", workDir);
                    boolean compressed = context.getServerChannelConfiguration().isStateTransferUseCompression();

                    try {
                        long logIndex;
                        try (OutputStream stream = createStream(file, compressed)) {
                            writeClientSessions(clientSessions, stream);
                            logIndex = context.getStateMachine().acquireSnapshot(stream);
                        }

                        long creationTime = Times.getCurrentTime();
                        String md5Hash = Files.md5Hash(file);
                        File snapshotFile = new File(workDir, logIndex + "-" + creationTime + "-" + md5Hash);
                        Assert.checkState(file.renameTo(snapshotFile));
                        return new Snapshot(snapshotFile, logIndex, creationTime);
                    } catch (Exception e) {
                        Files.delete(file);

                        return Exceptions.wrapAndThrow(e);
                    }
                }

                @Override
                public boolean isCanceled() {
                    return false;
                }

                @Override
                public void onSucceeded(Object result) {
                    replicationProtocol.unlockCommits();
                    completionHandler.onSucceeded(result);

                    if (logger.isLogEnabled(LogLevel.INFO))
                        logger.log(LogLevel.INFO, marker, messages.snapshotCreationCompleted((Snapshot) result));
                }

                @Override
                public void onFailed(Throwable error) {
                    replicationProtocol.unlockCommits();
                    completionHandler.onFailed(error);

                    if (logger.isLogEnabled(LogLevel.ERROR))
                        logger.log(LogLevel.ERROR, marker, messages.snapshotCreationFailed(), error);
                }
            });
        });

        Snapshot snapshot = completionHandler.await(v -> context.getCompartment().isStarted());

        synchronized (factory) {
            if (!closed) {
                this.snapshot = snapshot;
                this.completionHandler = null;
            } else {
                Files.delete(snapshot.getFile());
                Assert.checkState(false);
            }
        }

        return this.snapshot;
    }

    @Override
    public synchronized void removeSnapshot() {
        synchronized (factory) {
            Assert.checkState(!closed);

            if (snapshot != null) {
                if (logger.isLogEnabled(LogLevel.INFO))
                    logger.log(LogLevel.INFO, marker, messages.snapshotRemoved(snapshot));
            }

            snapshot = null;
            snapshotLoaded = true;
        }

        Files.emptyDir(workDir);
    }

    @Override
    public void close() {
        synchronized (factory) {
            if (!closed) {
                factory.clear(this, snapshot, snapshotLoaded);
                closed = true;
            }
        }
    }

    private void writeClientSessions(List<Pair<UUID, Long>> clientSessions, OutputStream stream) throws IOException {
        ByteOutputStream out = new ByteOutputStream();
        DataSerialization serialization = new DataSerialization(out);
        serialization.writeInt(clientSessions.size());
        for (Pair<UUID, Long> pair : clientSessions) {
            Serializers.writeUUID(serialization, pair.getKey());
            serialization.writeLong(pair.getValue());
        }

        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(out.getLength());
        buffer.flip();
        stream.write(buffer.array(), buffer.arrayOffset(), buffer.limit());
        stream.write(out.getBuffer(), 0, out.getLength());
    }

    private OutputStream createStream(File file, boolean compressed) throws IOException {
        OutputStream stream = new BufferedOutputStream(new FileOutputStream(file));
        if (compressed)
            stream = new GZIPOutputStream(stream);

        return stream;
    }

    private void checkSnapshot() {
        if (snapshot != null && snapshot.getLogIndex() < context.getLogStore().getStartIndex()) {
            if (logger.isLogEnabled(LogLevel.INFO))
                logger.log(LogLevel.INFO, marker, messages.snapshotOutdated(snapshot));

            removeSnapshot();
        }
    }

    private void loadSnapshot() {
        synchronized (factory) {
            if (snapshotLoaded)
                return;

            snapshotLoaded = true;
        }

        Snapshot snapshot;
        try {
            File[] files = workDir.listFiles();
            if (files.length == 0)
                return;

            Assert.checkState(files.length == 1);

            File file = files[0];
            String name = file.getName();
            String[] parts = name.split("-");
            Assert.checkState(parts.length == 3);

            long logIndex = Long.valueOf(parts[0]);
            long creationTime = Long.valueOf(parts[1]);
            String md5Hash = parts[2];

            Assert.checkState(md5Hash.equals(Files.md5Hash(file)));
            snapshot = new Snapshot(file, logIndex, creationTime);

            if (logger.isLogEnabled(LogLevel.INFO))
                logger.log(LogLevel.INFO, marker, messages.snapshotLoaded(snapshot));
        } catch (Exception e) {
            Files.emptyDir(workDir);
            snapshot = null;

            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, messages.snapshotLoadFailed(), e);
        }

        synchronized (factory) {
            this.snapshot = snapshot;
        }
    }

    private interface IMessages {
        @DefaultMessage("Snapshot creation has been started.")
        ILocalizedMessage snapshotCreationStarted();

        @DefaultMessage("Snapshot creation has been completed: {0}.")
        ILocalizedMessage snapshotCreationCompleted(Snapshot snapshot);

        @DefaultMessage("Snapshot creation has failed.")
        ILocalizedMessage snapshotCreationFailed();

        @DefaultMessage("Snapshot has been removed: {0}.")
        ILocalizedMessage snapshotRemoved(Snapshot snapshot);

        @DefaultMessage("Snapshot is outdated: {0}.")
        ILocalizedMessage snapshotOutdated(Snapshot snapshot);

        @DefaultMessage("Snapshot has been loaded: {0}.")
        ILocalizedMessage snapshotLoaded(Snapshot snapshot);

        @DefaultMessage("Snapshot loading has failed.")
        ILocalizedMessage snapshotLoadFailed();
    }
}
