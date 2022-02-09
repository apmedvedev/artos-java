package com.artos.impl.core.server.state;

import com.artos.api.core.server.conf.ServerChannelConfiguration;
import com.artos.impl.core.server.impl.Context;
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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

public class StateTransferServerTask implements IStateTransferAcquirer {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final Context context;
    private final StateTransferServer stateTransferServer;
    private final ISnapshotManagerFactory snapshotManagerFactory;

    public StateTransferServerTask(Context context, ISnapshotManagerFactory snapshotManagerFactory) {
        Assert.notNull(context);
        Assert.notNull(snapshotManagerFactory);

        this.context = context;
        this.snapshotManagerFactory = snapshotManagerFactory;
        this.marker = context.getMarker();

        ServerChannelConfiguration configuration = context.getServerChannelConfiguration();
        String workDir = new File(configuration.getWorkDir(), "state-server").getAbsolutePath();

        this.stateTransferServer = new StateTransferServer(context.getLocalServer().getEndpoint(), configuration.getStateTransferPortRangeStart(),
            configuration.getStateTransferPortRangeEnd(), configuration.getStateTransferBindAddress(), workDir,
            configuration.isSecured(), configuration.getKeyStoreFile(), configuration.getKeyStorePassword(), this,
            context.getCompartment());
    }

    public String getHost() {
        InetSocketAddress address = stateTransferServer.getLocalAddress();
        return address.getAddress().getCanonicalHostName();
    }

    public int getPort() {
        InetSocketAddress address = stateTransferServer.getLocalAddress();
        return address.getPort();
    }

    public void start() {
        stateTransferServer.start();
    }

    public void stop() {
        stateTransferServer.stop();
    }

    @Override
    public StateInfo acquireState(UUID groupId, long logIndex, File workDir) {
        Assert.checkState(groupId.equals(context.getGroupId()));

        try {
            context.getLogStore().lockCompaction();

            StateInfo stateInfo = new StateInfo();
            if (logIndex >= context.getLogStore().getStartIndex())
                acquireLog(logIndex, workDir, stateInfo);
            else
                acquireSnapshot(workDir, stateInfo);

            return stateInfo;
        } finally {
            context.getLogStore().unlockCompaction();
        }
    }

    private void acquireSnapshot(File workDir, StateInfo stateInfo) {
        boolean compressed = context.getServerChannelConfiguration().isStateTransferUseCompression();

        Snapshot snapshot;
        try (ISnapshotManager snapshotManager = snapshotManagerFactory.create()) {
            snapshot = snapshotManager.getSnapshot();
            if (snapshot == null) {
                snapshot = snapshotManager.createSnapshot();
            }

            File file = Files.createTempFile("raft", "tmp", workDir);
            Files.copy(snapshot.getFile(), file);

            SnapshotState snapshotState = new SnapshotState();
            snapshotState.compressed = compressed;
            snapshotState.logIndex = snapshot.getLogIndex();
            snapshotState.file = file;

            stateInfo.snapshotState = snapshotState;

            if (logger.isLogEnabled(LogLevel.INFO))
                logger.log(LogLevel.INFO, marker, messages.acquiredSnapshot(snapshot));
        } catch (Exception e) {
            Exceptions.wrapAndThrow(e);
            return;
        }

        acquireLog(snapshot.getLogIndex(), workDir, stateInfo);
    }

    private void acquireLog(long logIndex, File workDir, StateInfo stateInfo) {
        boolean compressed = context.getServerChannelConfiguration().isStateTransferUseCompression();
        long maxSize = context.getServerChannelConfiguration().getStateTransferMaxSize();

        stateInfo.logStates = new ArrayList<>();
        while (true) {
            File file = Files.createTempFile("raft", "tmp", workDir);
            try (OutputStream stream = createStream(file, compressed)) {
                logIndex = context.getLogStore().read(logIndex, stream, maxSize);
            } catch (IOException e) {
                Exceptions.wrapAndThrow(e);
            }

            LogState state = new LogState();
            state.file = file;
            state.compressed = compressed;

            stateInfo.logStates.add(state);

            if (logger.isLogEnabled(LogLevel.INFO))
                logger.log(LogLevel.INFO, marker, messages.acquiredLog(file, logIndex, compressed));

            if (logIndex == -1)
                break;
        }
    }

    private OutputStream createStream(File file, boolean compressed) throws IOException {
        OutputStream stream = new BufferedOutputStream(new FileOutputStream(file));
        if (compressed)
            stream = new GZIPOutputStream(stream);

        return stream;
    }

    private interface IMessages {
        @DefaultMessage("Snapshot has been acquired: {0}.")
        ILocalizedMessage acquiredSnapshot(Snapshot snapshot);

        @DefaultMessage("Log has been acquired. File: {0}, next-log-index: {1}, compressed: {2}")
        ILocalizedMessage acquiredLog(File file, long logIndex, boolean compressed);
    }
}
