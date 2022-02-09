package com.artos.impl.core.server.channel;

import com.artos.api.core.server.IServerChannelFactory;
import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerChannelConfiguration;
import com.artos.api.core.server.conf.ServerChannelFactoryConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.impl.core.server.client.LeaderClientProtocol;
import com.artos.impl.core.server.election.ElectionProtocol;
import com.artos.impl.core.server.election.IElectionProtocol;
import com.artos.impl.core.server.election.NoOpElectionProtocol;
import com.artos.impl.core.server.impl.Context;
import com.artos.impl.core.server.impl.Server;
import com.artos.impl.core.server.impl.State;
import com.artos.impl.core.server.membership.IJoinGroupProtocol;
import com.artos.impl.core.server.membership.ILeaveGroupProtocol;
import com.artos.impl.core.server.membership.JoinGroupProtocol;
import com.artos.impl.core.server.membership.LeaveGroupProtocol;
import com.artos.impl.core.server.membership.MembershipProtocol;
import com.artos.impl.core.server.membership.MembershipService;
import com.artos.impl.core.server.membership.NoOpJoinGroupProtocol;
import com.artos.impl.core.server.membership.NoOpLeaveGroupProtocol;
import com.artos.impl.core.server.replication.QueryProtocol;
import com.artos.impl.core.server.replication.ReplicationProtocol;
import com.artos.impl.core.server.state.IStateTransferProtocol;
import com.artos.impl.core.server.state.NoOpStateTransferProtocol;
import com.artos.impl.core.server.state.StateTransferProtocol;
import com.artos.spi.core.IGroupChannel;
import com.artos.spi.core.ILogStore;
import com.artos.spi.core.ILogStoreFactory;
import com.artos.spi.core.IMessageListenerFactory;
import com.artos.spi.core.IMessageSenderFactory;
import com.artos.spi.core.IStateMachine;
import com.artos.spi.core.IStateMachineFactory;
import com.exametrika.common.compartment.ICompartment;
import com.exametrika.common.compartment.ICompartmentFactory;
import com.exametrika.common.compartment.ICompartmentFactory.Parameters;
import com.exametrika.common.compartment.impl.CompartmentFactory;
import com.exametrika.common.utils.Assert;

public class ServerChannelFactory implements IServerChannelFactory {
    private final ServerChannelFactoryConfiguration factoryConfiguration;

    public ServerChannelFactory(ServerChannelFactoryConfiguration factoryConfiguration) {
        Assert.notNull(factoryConfiguration);

        this.factoryConfiguration = factoryConfiguration;
    }

    @Override
    public ServerChannel createChannel(ServerChannelConfiguration configuration, IStateMachineFactory stateMachineFactory) {
        Assert.notNull(configuration);
        Assert.notNull(stateMachineFactory);

        ServerConfiguration serverConfiguration = configuration.getServer();
        GroupConfiguration groupConfiguration = configuration.getGroup();

        boolean singleServer = groupConfiguration.isSingleServer();

        Parameters compartmentParameters = new ICompartmentFactory.Parameters();
        compartmentParameters.name = serverConfiguration.getEndpoint();
        compartmentParameters.dispatchPeriod = factoryConfiguration.getTimerPeriod();
        ICompartment compartment = new CompartmentFactory().createCompartment(compartmentParameters);

        State state = new State();
        state.setConfiguration(groupConfiguration);

        LeaderClientProtocol leaderClientProtocol = new LeaderClientProtocol(state, factoryConfiguration.getMaxValueSize(),
            factoryConfiguration.getLockQueueCapacity(), factoryConfiguration.getUnlockQueueCapacity(),
            factoryConfiguration.getCommitPeriod());

        MembershipService membershipService = new MembershipService(state);

        ILogStoreFactory logStoreFactory = configuration.getLogStore().createFactory();
        ILogStore logStore = logStoreFactory.createLogStore();

        IGroupChannel groupChannel = new GroupChannel(leaderClientProtocol, membershipService);

        IStateMachine stateMachine = stateMachineFactory.createStateMachine(groupChannel);
        IMessageListenerFactory messageListenerFactory = configuration.getMessageListener().createFactory();
        IMessageSenderFactory messageSenderFactory = configuration.getMessageSender().createFactory();

        Context context = new Context(serverConfiguration, groupConfiguration.getGroupId(),
            logStore, stateMachine, factoryConfiguration, configuration, messageListenerFactory, messageSenderFactory, compartment);

        membershipService.setContext(context);

        Server server = new Server(context, state);
        compartment.addTimerProcessor(server);

        IElectionProtocol electionProtocol;
        IJoinGroupProtocol joinGroupProtocol;
        ILeaveGroupProtocol leaveGroupProtocol;
        IStateTransferProtocol stateTransferProtocol;

        if (singleServer) {
            electionProtocol = new NoOpElectionProtocol(context, state);
            joinGroupProtocol = new NoOpJoinGroupProtocol();
            leaveGroupProtocol = new NoOpLeaveGroupProtocol();
            stateTransferProtocol = new NoOpStateTransferProtocol();
        } else {
            electionProtocol = new ElectionProtocol(server, context, state);
            joinGroupProtocol = new JoinGroupProtocol(context, state);
            leaveGroupProtocol = new LeaveGroupProtocol(context, state);
            stateTransferProtocol = new StateTransferProtocol(context, state);
        }

        ReplicationProtocol replicationProtocol = new ReplicationProtocol(server, context, state);
        MembershipProtocol membershipProtocol = new MembershipProtocol(server, context, state);
        QueryProtocol queryProtocol = new QueryProtocol(context, replicationProtocol.getClientSessionManager());

        server.setElectionProtocol(electionProtocol);
        server.setJoinGroupProtocol(joinGroupProtocol);
        server.setLeaveGroupProtocol(leaveGroupProtocol);
        server.setReplicationProtocol(replicationProtocol);
        server.setStateTransferProtocol(stateTransferProtocol);
        server.setLeaderClientProtocol(leaderClientProtocol);
        server.setQueryProtocol(queryProtocol);

        if (singleServer) {
            ((NoOpElectionProtocol)electionProtocol).setMembershipProtocol(membershipProtocol);
        } else {
            ((ElectionProtocol)electionProtocol).setMembershipProtocol(membershipProtocol);
            ((ElectionProtocol)electionProtocol).setReplicationProtocol(replicationProtocol);

            ((JoinGroupProtocol)joinGroupProtocol).setMembershipProtocol(membershipProtocol);
            ((JoinGroupProtocol)joinGroupProtocol).setReplicationProtocol(replicationProtocol);
            ((JoinGroupProtocol)joinGroupProtocol).setStateTransferProtocol(stateTransferProtocol);

            ((LeaveGroupProtocol)leaveGroupProtocol).setMembershipProtocol(membershipProtocol);
            ((LeaveGroupProtocol)leaveGroupProtocol).setReplicationProtocol(replicationProtocol);
            ((LeaveGroupProtocol)leaveGroupProtocol).setElectionProtocol(electionProtocol);

            ((StateTransferProtocol)stateTransferProtocol).setMembershipProtocol(membershipProtocol);
            ((StateTransferProtocol)stateTransferProtocol).setReplicationProtocol(replicationProtocol);
        }

        replicationProtocol.setElectionProtocol(electionProtocol);
        replicationProtocol.setMembershipProtocol(membershipProtocol);
        replicationProtocol.setStateTransferProtocol(stateTransferProtocol);
        replicationProtocol.setQueryProtocol(queryProtocol);

        membershipProtocol.setJoinGroupProtocol(joinGroupProtocol);
        membershipProtocol.setLeaveGroupProtocol(leaveGroupProtocol);
        membershipProtocol.setMembershipService(membershipService);
        membershipProtocol.setReplicationProtocol(replicationProtocol);
        membershipProtocol.setStateTransferProtocol(stateTransferProtocol);
        membershipProtocol.setLeaderClientProtocol(leaderClientProtocol);
        membershipProtocol.setQueryProtocol(queryProtocol);

        leaderClientProtocol.setReplicationProtocol(replicationProtocol);
        leaderClientProtocol.setFlowController(stateMachine);
        leaderClientProtocol.setContext(context);

        return new ServerChannel(server, context, leaveGroupProtocol, membershipService);
    }
}
