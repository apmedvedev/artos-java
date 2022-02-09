package com.artos.impl.core.server.election;

import com.artos.impl.core.message.VoteRequest;
import com.artos.impl.core.message.VoteResponse;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.ServerRole;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.membership.IMembershipProtocol;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.utils.Assert;

public class NoOpElectionProtocol implements IElectionProtocol {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final Context context;
    private final State state;
    private IMembershipProtocol membershipProtocol;

    public NoOpElectionProtocol(Context context, State state) {
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

    @Override
    public void delayElection() {
    }

    @Override
    public void becomeFollower() {
    }

    @Override
    public void lockElection() {
    }

    @Override
    public VoteResponse handleVoteRequest(VoteRequest request) {
        Assert.supports(false);
        return null;
    }

    @Override
    public void handleVoteResponse(VoteResponse response) {
        Assert.supports(false);
    }

    @Override
    public void onTimer(long currentTime) {
    }

    @Override
    public void start() {
        state.setRole(ServerRole.LEADER);
        state.setLeader(context.getServerId());

        membershipProtocol.onLeader();

        if (logger.isLogEnabled(LogLevel.INFO))
            logger.log(LogLevel.INFO, marker, messages.serverElectedAsLeader(state.getServerState().getTerm()));
    }

    @Override
    public void stop() {
    }

    private interface IMessages {
        @DefaultMessage("Server is elected as a leader for term {0}.")
        ILocalizedMessage serverElectedAsLeader(long term);
    }
}
