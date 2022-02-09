package com.artos.impl.core.server.state;

import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.membership.IMembershipProtocol;
import com.artos.impl.core.server.replication.IReplicationProtocol;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.l10n.SystemException;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.CompletionHandler;
import com.exametrika.common.utils.ICompletionHandler;
import com.exametrika.common.utils.Times;

public class StateTransferProtocol implements IStateTransferProtocol {
    private static final IMessages messages = Messages.get(IMessages.class);
    private static final long MIN_STATE_TRANSFER_PERIOD = 3000;
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final Context context;
    private final State state;
    private StateTransferClientTask stateTransferClientTask;
    private StateTransferServerTask stateTransferServerTask;
    private SnapshotManagerFactory snapshotManagerFactory;
    private IMembershipProtocol membershipProtocol;
    private IReplicationProtocol replicationProtocol;
    private long lastStateTransferTime;

    public StateTransferProtocol(Context context, State state) {
        Assert.notNull(context);
        Assert.notNull(state);

        this.context = context;
        this.state = state;
        this.marker = context.getMarker();
    }

    public void setMembershipProtocol(IMembershipProtocol membershipProtocol) {
        Assert.notNull(membershipProtocol);
        Assert.isNull(this.membershipProtocol);

        this.membershipProtocol = membershipProtocol;
    }

    public void setReplicationProtocol(IReplicationProtocol replicationProtocol) {
        Assert.notNull(replicationProtocol);
        Assert.isNull(this.replicationProtocol);

        this.replicationProtocol = replicationProtocol;
    }

    @Override
    public void requestStateTransfer(String host, int port, long startLogIndex, ICompletionHandler completionHandler) {
        long currentTime = Times.getCurrentTime();
        if (stateTransferClientTask != null || currentTime < lastStateTransferTime + MIN_STATE_TRANSFER_PERIOD) {
            if (completionHandler != null)
                completionHandler.onFailed(new SystemException(messages.clientStateTransferInProgress()));

            return;
        }

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.clientStateTransferStarted(host, port, startLogIndex));

        stateTransferClientTask = new StateTransferClientTask(host, port, startLogIndex, state, context, new CompletionHandler() {
            @Override
            public void onSucceeded(Object result) {
                if (completionHandler != null)
                    completionHandler.onSucceeded(result);

                stateTransferClientTask = null;
                lastStateTransferTime = Times.getCurrentTime();

                if (logger.isLogEnabled(LogLevel.INFO))
                    logger.log(LogLevel.INFO, marker, messages.clientStateTransferCompleted());
            }

            @Override
            public void onFailed(Throwable error) {
                if (completionHandler != null)
                    completionHandler.onFailed(error);

                stateTransferClientTask = null;
                lastStateTransferTime = Times.getCurrentTime();

                if (logger.isLogEnabled(LogLevel.ERROR))
                    logger.log(LogLevel.ERROR, marker, messages.clientStateTransferFailed(), error);
            }
        }, membershipProtocol, replicationProtocol);

        context.getCompartment().execute(stateTransferClientTask);
    }

    @Override
    public void onLeader() {
        snapshotManagerFactory = new SnapshotManagerFactory(context);
        snapshotManagerFactory.setReplicationProtocol(replicationProtocol);
        snapshotManagerFactory.start();

        stateTransferServerTask = new StateTransferServerTask(context, snapshotManagerFactory);
        stateTransferServerTask.start();
    }

    @Override
    public void onFollower() {
        if (stateTransferServerTask != null) {
            stateTransferServerTask.stop();
            stateTransferServerTask = null;
        }

        if (snapshotManagerFactory != null) {
            snapshotManagerFactory.uninstall();
            snapshotManagerFactory = null;
        }

    }

    @Override
    public String getHost() {
        Assert.checkState(stateTransferServerTask != null);
        return stateTransferServerTask.getHost();
    }

    @Override
    public int getPort() {
        Assert.checkState(stateTransferServerTask != null);
        return stateTransferServerTask.getPort();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        if (stateTransferServerTask != null)
            stateTransferServerTask.stop();

        if (snapshotManagerFactory != null)
            snapshotManagerFactory.stop();

        if (stateTransferClientTask != null)
            stateTransferClientTask.stop();
    }

    private interface IMessages {
        @DefaultMessage("Client state transfer is in progress.")
        ILocalizedMessage clientStateTransferInProgress();

        @DefaultMessage("Client state transfer has been started. Host: {0}, port: {1}, start-log-index: {2}")
        ILocalizedMessage clientStateTransferStarted(String host, int port, long startLogIndex);

        @DefaultMessage("Client state transfer has been completed.")
        ILocalizedMessage clientStateTransferCompleted();

        @DefaultMessage("Client state transfer has failed.")
        ILocalizedMessage clientStateTransferFailed();
    }
}
