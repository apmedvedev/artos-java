package com.artos.impl.core.server.channel;

import com.artos.api.core.server.IMembershipListener;
import com.artos.api.core.server.IServerChannel;
import com.artos.api.core.server.IServerChannelListener;
import com.artos.api.core.server.StopType;
import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerChannelConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.Server;
import com.artos.impl.core.server.membership.ILeaveGroupProtocol;
import com.artos.impl.core.server.membership.MembershipService;
import com.artos.spi.core.IStateMachine;
import com.exametrika.common.compartment.ICompartment;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.tasks.ThreadInterruptedException;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Exceptions;
import com.exametrika.common.utils.SyncCompletionHandler;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class ServerChannel implements IServerChannel {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final Server server;
    private final ICompartment compartment;
    private final ILeaveGroupProtocol leaveGroupProtocol;
    private final Context context;
    private final MembershipService membershipService;
    private volatile boolean started;
    private volatile Set<IServerChannelListener> channelListeners = new HashSet<>();

    public ServerChannel(Server server, Context context, ILeaveGroupProtocol leaveGroupProtocol, MembershipService membershipService) {
        Assert.notNull(server);
        Assert.notNull(context);
        Assert.notNull(leaveGroupProtocol);
        Assert.notNull(membershipService);

        this.server = server;
        this.context = context;
        this.compartment = context.getCompartment();
        this.leaveGroupProtocol = leaveGroupProtocol;
        this.membershipService = membershipService;
        this.marker = context.getMarker();
    }

    @Override
    public ServerChannelConfiguration getConfiguration() {
        return context.getServerChannelConfiguration();
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public IStateMachine getStateMachine() {
        return context.getStateMachine();
    }

    @Override
    public synchronized void addChannelListener(IServerChannelListener listener) {
        Assert.notNull(listener);

        if (channelListeners.contains(listener))
            return;

        Set<IServerChannelListener> listeners = new HashSet<>(channelListeners);
        listeners.add(listener);

        channelListeners = listeners;
    }

    @Override
    public synchronized void removeChannelListener(IServerChannelListener listener) {
        Assert.notNull(listener);

        if (!channelListeners.contains(listener))
            return;

        Set<IServerChannelListener> listeners = new HashSet<>(channelListeners);
        listeners.remove(listener);

        channelListeners = listeners;
    }

    @Override
    public synchronized void removeAllChannelListeners() {
        channelListeners = new HashSet<>();
    }

    @Override
    public void start() {
        start(Integer.MAX_VALUE);
    }

    @Override
    public void stop() {
        stop(Integer.MAX_VALUE, StopType.GRACEFULLY);
    }

    @Override
    public void stop(long timeout, StopType stopType) {
        Assert.notNull(stopType);

        switch (stopType) {
            case LEAVE_GROUP:
                leaveGroup(timeout);
                gracefulStop(timeout);
                break;
            case GRACEFULLY:
                gracefulStop(timeout);
                break;
            case IMMEDIATE:
                break;
            default:
                Assert.error();
        }

        stop(timeout);
    }

    @Override
    public ServerConfiguration getLocalServer() {
        return membershipService.getLocalServer();
    }

    @Override
    public GroupConfiguration getGroup() {
        return membershipService.getGroup();
    }

    @Override
    public UUID getLeader() {
        return membershipService.getLeader();
    }

    @Override
    public void addMembershipListener(IMembershipListener listener) {
        membershipService.addMembershipListener(listener);
    }

    @Override
    public void removeMembershipListener(IMembershipListener listener) {
        membershipService.removeMembershipListener(listener);
    }

    @Override
    public void removeAllMembershipListeners() {
        membershipService.removeAllMembershipListeners();
    }

    private void leaveGroup(long timeout) {
        try {
            SyncCompletionHandler completionHandler = new SyncCompletionHandler();
            compartment.offer(() -> leaveGroupProtocol.leaveGroup(completionHandler));
            completionHandler.await(timeout);
        } catch (Exception e) {
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);
        }
    }

    private void gracefulStop(long timeout) {
        try {
            SyncCompletionHandler completionHandler = new SyncCompletionHandler();
            compartment.offer(() -> leaveGroupProtocol.gracefulClose(completionHandler));
            completionHandler.await(timeout);
        } catch (Exception e) {
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);
        }
    }

    private void start(long timeout) {
        compartment.start();

        try {
            SyncCompletionHandler completionHandler = new SyncCompletionHandler();
            compartment.offer(() -> {
                try {
                    server.start();
                    completionHandler.onSucceeded(true);
                } catch (Exception e) {
                    completionHandler.onFailed(e);
                }
            });
            completionHandler.await(timeout);
        } catch (Exception e) {
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);
        }

        started = true;
        onStarted();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.channelStarted());
    }

    private void stop(long timeout) {
        try {
            SyncCompletionHandler completionHandler = new SyncCompletionHandler();
            compartment.offer(() -> {
                try {
                    server.stop();
                    completionHandler.onSucceeded(true);
                } catch (Exception e) {
                    completionHandler.onFailed(e);
                }
            });
            completionHandler.await(timeout);
        } catch (Exception e) {
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);
        }

        compartment.stop();
        started = false;
        onStopped();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.channelStopped());
    }

    private void onStarted() {
        for (IServerChannelListener listener : channelListeners)
            onStarted(listener);
    }

    private void onStopped() {
        for (IServerChannelListener listener : channelListeners)
            onStopped(listener);
    }

    private void onStarted(IServerChannelListener listener) {
        try {
            listener.onStarted();
        } catch (ThreadInterruptedException e) {
            throw e;
        } catch (Exception e) {
            Exceptions.checkInterrupted(e);

            // Isolate exception from other listeners
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);
        }
    }

    private void onStopped(IServerChannelListener listener) {
        try {
            listener.onStopped();
        } catch (ThreadInterruptedException e) {
            throw e;
        } catch (Exception e) {
            Exceptions.checkInterrupted(e);

            // Isolate exception from other listeners
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);
        }
    }

    private interface IMessages {
        @DefaultMessage("Server channel has been started.")
        ILocalizedMessage channelStarted();

        @DefaultMessage("Server channel has been stopped.")
        ILocalizedMessage channelStopped();
    }
}
