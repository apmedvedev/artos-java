package com.artos.impl.core.server.state;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerChannelConfiguration;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.membership.IMembershipProtocol;
import com.artos.impl.core.server.replication.ClientSession;
import com.artos.impl.core.server.replication.ClientSessionManager;
import com.artos.impl.core.server.replication.IReplicationProtocol;
import com.artos.spi.core.IStateMachineTransaction;
import com.exametrika.common.compartment.impl.CompletionCompartmentTask;
import com.exametrika.common.io.impl.ByteInputStream;
import com.exametrika.common.io.impl.DataDeserialization;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Exceptions;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.IOs;
import com.exametrika.common.utils.Pair;
import com.exametrika.common.utils.Serializers;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

public class StateTransferClientTask extends CompletionCompartmentTask implements IStateTransferApplier {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final String host;
    private final int port;
    private final long startLogIndex;
    private final State state;
    private final Context context;
    private final IMembershipProtocol membershipProtocol;
    private final GroupConfiguration configuration;
    private final IReplicationProtocol replicationProtocol;
    private volatile StateTransferClient stateTransferClient;
    private volatile long commitIndex;
    private volatile List<Pair<UUID, Long>> clientSessions;

    public StateTransferClientTask(String host, int port, long startLogIndex, State state, Context context,
                                   ICompletionHandler completionHandler, IMembershipProtocol membershipProtocol,
                                   IReplicationProtocol replicationProtocol) {
        super(completionHandler);

        Assert.notNull(host);
        Assert.notNull(context);
        Assert.notNull(state);
        Assert.notNull(membershipProtocol);
        Assert.notNull(replicationProtocol);

        this.host = host;
        this.port = port;
        this.startLogIndex = startLogIndex;
        this.state = state;
        this.context = context;
        this.membershipProtocol = membershipProtocol;
        this.configuration = state.getConfiguration();
        this.replicationProtocol = replicationProtocol;
        this.marker = context.getMarker();
    }

    @Override
    public Object execute() {
        ServerChannelConfiguration configuration = context.getServerChannelConfiguration();
        String workDir = new File(configuration.getWorkDir(), "state-client").getAbsolutePath();

        stateTransferClient = new StateTransferClient(context.getLocalServer().getEndpoint(), host, port, workDir,
            configuration.isSecured(), configuration.getKeyStoreFile(), configuration.getKeyStorePassword(), this);

        try {
            context.getLogStore().lockCompaction();

            stateTransferClient.transfer(context.getGroupId(), startLogIndex);
        } finally {
            context.getLogStore().unlockCompaction();
        }

        return true;
    }

    @Override
    public void applySnapshot(File stateFile, long logIndex, boolean compressed) {
        context.getLogStore().clear(logIndex);
        commitIndex = logIndex;

        try (InputStream stream = createStream(stateFile, compressed)) {
            readClientSessions(stream);
            context.getStateMachine().applySnapshot(logIndex, stream);
        } catch (IOException e) {
            Exceptions.wrapAndThrow(e);
        }

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.appliedSnapshot(stateFile, logIndex, compressed));
    }

    @Override
    public void applyLog(File stateFile, boolean compressed) {
        try (InputStream stream = createStream(stateFile, compressed)) {
            context.getLogStore().write(stream);
        } catch (IOException e) {
            Exceptions.wrapAndThrow(e);
        }

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.appliedLog(stateFile, compressed));
    }

    public void stop() {
        if (stateTransferClient != null)
            stateTransferClient.close();
    }

    @Override
    protected void onCompleted(Object result) {
        IStateMachineTransaction transaction = context.getStateMachine().beginTransaction(true);
        GroupConfiguration newConfiguration = transaction.readConfiguration();
        transaction.commit();

        if (!configuration.equals(newConfiguration))
            membershipProtocol.reconfigure(newConfiguration);

        if (commitIndex > 0) {
            state.getServerState().setCommitIndex(commitIndex);
            state.setQuickCommitIndex(commitIndex);
        }

        List<Pair<UUID, Long>> clientSessions = this.clientSessions;
        if (clientSessions != null) {
            ClientSessionManager clientSessionManager = replicationProtocol.getClientSessionManager();

            for (Pair<UUID, Long> pair : clientSessions) {
                UUID clientId = pair.getKey();
                long lastCommittedMessageId = pair.getValue();

                ClientSession session = clientSessionManager.ensureSession(clientId);
                if (session.getLastCommittedMessageId() < lastCommittedMessageId) {
                    session.commit(lastCommittedMessageId);
                    session.setCommitted(false);
                }
            }
        }
    }

    private void readClientSessions(InputStream stream) throws IOException {
        byte[] buf = new byte[4];
        stream.read(buf);
        ByteBuffer buffer = ByteBuffer.wrap(buf);
        int length = buffer.getInt();

        buf = new byte[length];
        IOs.readFully(stream, buf, 0, buf.length);
        ByteInputStream in = new ByteInputStream(buf);
        DataDeserialization deserialization = new DataDeserialization(in);
        int count = deserialization.readInt();
        List<Pair<UUID, Long>> clientSessions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            UUID clientId = Serializers.readUUID(deserialization);
            long lastCommittedMessageId = deserialization.readLong();

            clientSessions.add(new Pair<>(clientId, lastCommittedMessageId));
        }

        this.clientSessions = clientSessions;
    }

    private InputStream createStream(File file, boolean compressed) throws IOException {
        InputStream stream = new BufferedInputStream(new FileInputStream(file));
        if (compressed)
            stream = new GZIPInputStream(stream);

        return stream;
    }

    private interface IMessages {
        @DefaultMessage("Snapshot has been applied. File: {0}, log-index: {1}, compressed: {2}")
        ILocalizedMessage appliedSnapshot(File stateFile, long logIndex, boolean compressed);

        @DefaultMessage("Log has been applied. File: {0}, compressed: {1}")
        ILocalizedMessage appliedLog(File stateFile, boolean compressed);
    }
}
