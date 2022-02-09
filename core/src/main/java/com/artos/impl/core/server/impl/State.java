package com.artos.impl.core.server.impl;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.spi.core.ServerState;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Immutables;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class State {
    private GroupConfiguration configuration;
    private ServerRole role = ServerRole.FOLLOWER;
    private ServerState serverState;
    private UUID leader;
    private final Map<UUID, IServerPeer> peers = Immutables.wrap(new HashMap<>());
    private long quickCommitIndex;
    private boolean configurationChanging;
    private boolean catchingUp;

    public GroupConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(GroupConfiguration configuration) {
        this.configuration = configuration;
    }

    public ServerRole getRole() {
        return role;
    }

    public void setRole(ServerRole role) {
        this.role = role;
    }

    public ServerState getServerState() {
        return serverState;
    }

    public void setServerState(ServerState serverState) {
        this.serverState = serverState;
    }

    public UUID getLeader() {
        return leader;
    }

    public void setLeader(UUID leader) {
        this.leader = leader;
    }

    public Map<UUID, IServerPeer> getPeers() {
        return peers;
    }

    public void addPeer(IServerPeer peer) {
        Assert.checkState(Immutables.unwrap(peers).put(peer.getConfiguration().getId(), peer) == null);
    }

    public void removePeer(UUID id) {
        Immutables.unwrap(peers).remove(id);
    }

    public long getQuickCommitIndex() {
        return quickCommitIndex;
    }

    public void setQuickCommitIndex(long quickCommitIndex) {
        this.quickCommitIndex = quickCommitIndex;
    }

    public boolean isConfigurationChanging() {
        return configurationChanging;
    }

    public void setConfigurationChanging(boolean configurationChanging) {
        this.configurationChanging = configurationChanging;
    }

    public boolean isCatchingUp() {
        return catchingUp;
    }

    public void setCatchingUp(boolean catchingUp) {
        this.catchingUp = catchingUp;
    }
}
