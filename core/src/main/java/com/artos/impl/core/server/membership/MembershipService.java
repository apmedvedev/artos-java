package com.artos.impl.core.server.membership;

import com.artos.api.core.server.IMembershipListener;
import com.artos.api.core.server.IMembershipService;
import com.artos.api.core.server.MembershipChangeEvent;
import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.State;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.tasks.ThreadInterruptedException;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Exceptions;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class MembershipService implements IMembershipService {
    private final ILogger logger = Loggers.get(getClass());
    private IMarker marker;
    private final State state;
    private Context context;
    private volatile Set<IMembershipListener> membershipListeners = new HashSet<>();

    public MembershipService(State state) {
        Assert.notNull(state);

        this.state = state;
    }

    public void setContext(Context context) {
        Assert.notNull(context);
        Assert.isNull(this.context);

        this.context = context;
        marker = context.getMarker();
    }

    @Override
    public ServerConfiguration getLocalServer() {
        return context.getLocalServer();
    }

    @Override
    public GroupConfiguration getGroup() {
        return state.getConfiguration();
    }

    @Override
    public UUID getLeader() {
        return state.getLeader();
    }

    @Override
    public synchronized void addMembershipListener(IMembershipListener listener) {
        Assert.notNull(listener);

        if (membershipListeners.contains(listener))
            return;

        Set<IMembershipListener> listeners = new HashSet<>(membershipListeners);
        listeners.add(listener);

        membershipListeners = listeners;
    }

    @Override
    public synchronized void removeMembershipListener(IMembershipListener listener) {
        Assert.notNull(listener);

        if (!membershipListeners.contains(listener))
            return;

        Set<IMembershipListener> listeners = new HashSet<>(membershipListeners);
        listeners.remove(listener);

        membershipListeners = listeners;
    }

    @Override
    public synchronized void removeAllMembershipListeners() {
        membershipListeners = new HashSet<>();
    }

    public void onLeader() {
        for (IMembershipListener listener : membershipListeners)
            onLeader(listener);
    }

    public void onFollower() {
        for (IMembershipListener listener : membershipListeners)
            onFollower(listener);
    }

    public void onJoined() {
        for (IMembershipListener listener : membershipListeners)
            onJoined(listener);
    }

    public void onLeft() {
        for (IMembershipListener listener : membershipListeners)
            onLeft(listener);
    }

    public void onMembershipChanged(MembershipChangeEvent event) {
        for (IMembershipListener listener : membershipListeners)
            onMembershipChanged(listener, event);
    }

    private void onLeader(IMembershipListener listener) {
        try {
            listener.onLeader();
        } catch (ThreadInterruptedException e) {
            throw e;
        } catch (Exception e) {
            Exceptions.checkInterrupted(e);

            // Isolate exception from other listeners
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);
        }
    }

    private void onFollower(IMembershipListener listener) {
        try {
            listener.onFollower();
        } catch (ThreadInterruptedException e) {
            throw e;
        } catch (Exception e) {
            Exceptions.checkInterrupted(e);

            // Isolate exception from other listeners
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);
        }
    }

    private void onJoined(IMembershipListener listener) {
        try {
            listener.onJoined();
        } catch (ThreadInterruptedException e) {
            throw e;
        } catch (Exception e) {
            Exceptions.checkInterrupted(e);

            // Isolate exception from other listeners
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);
        }
    }

    private void onLeft(IMembershipListener listener) {
        try {
            listener.onLeft();
        } catch (ThreadInterruptedException e) {
            throw e;
        } catch (Exception e) {
            Exceptions.checkInterrupted(e);

            // Isolate exception from other listeners
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);
        }
    }

    private void onMembershipChanged(IMembershipListener listener, MembershipChangeEvent event) {
        try {
            listener.onMembershipChanged(event);
        } catch (ThreadInterruptedException e) {
            throw e;
        } catch (Exception e) {
            Exceptions.checkInterrupted(e);

            // Isolate exception from other listeners
            if (logger.isLogEnabled(LogLevel.ERROR))
                logger.log(LogLevel.ERROR, marker, e);
        }
    }
}
