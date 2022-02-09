/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.api.core.server.conf;

import com.artos.api.core.server.IServerChannelFactory;
import com.artos.impl.core.server.channel.ServerChannelFactory;
import com.exametrika.common.config.Configuration;
import com.exametrika.common.utils.Objects;

public class ServerChannelFactoryConfiguration extends Configuration {
    private final int timerPeriod;
    private final int maxElectionTimeout;
    private final int minElectionTimeout;
    private final int heartbeatPeriod;
    private final int peerTimeout;
    private final int sendFailureBackoff;
    private final int maxPublishingLogEntryCount;
    private final int minStateTransferGapLogEntryCount;
    private final int clientSessionTimeout;
    private final int joinGroupPeriod;
    private final int leaveGroupPeriod;
    private final int leaveGroupAttemptCount;
    private final int autoLeaveGroupTimeout;
    private final int maxValueSize;
    private final int lockQueueCapacity;
    private final int unlockQueueCapacity;
    private final long commitPeriod;
    private final long subscriptionTimeout;

    public ServerChannelFactoryConfiguration(int timerPeriod, int maxElectionTimeout, int minElectionTimeout, int heartbeatPeriod,
                                             int peerTimeout, int sendFailureBackoff, int maxPublishingLogEntryCount, int minStateTransferGapLogEntryCount,
                                             int clientSessionTimeout, int joinGroupPeriod, int leaveGroupPeriod,
                                             int leaveGroupAttemptCount, int autoLeaveGroupTimeout,
                                             int maxValueSize, int lockQueueCapacity, int unlockQueueCapacity, long commitPeriod, long subscriptionTimeout) {
        this.timerPeriod = timerPeriod;
        this.maxElectionTimeout = maxElectionTimeout;
        this.minElectionTimeout = minElectionTimeout;
        this.heartbeatPeriod = heartbeatPeriod;
        this.peerTimeout = peerTimeout;
        this.sendFailureBackoff = sendFailureBackoff;
        this.maxPublishingLogEntryCount = maxPublishingLogEntryCount;
        this.minStateTransferGapLogEntryCount = minStateTransferGapLogEntryCount;
        this.clientSessionTimeout = clientSessionTimeout;
        this.joinGroupPeriod = joinGroupPeriod;
        this.leaveGroupPeriod = leaveGroupPeriod;
        this.leaveGroupAttemptCount = leaveGroupAttemptCount;
        this.autoLeaveGroupTimeout = autoLeaveGroupTimeout;
        this.maxValueSize = maxValueSize;
        this.lockQueueCapacity = lockQueueCapacity;
        this.unlockQueueCapacity = unlockQueueCapacity;
        this.commitPeriod = commitPeriod;
        this.subscriptionTimeout = subscriptionTimeout;
    }

    public int getTimerPeriod() {
        return timerPeriod;
    }

    public int getMaxHeartbeatPeriod() {
        return Math.max(this.heartbeatPeriod, this.minElectionTimeout - this.heartbeatPeriod / 2);
    }

    public int getMaxElectionTimeout() {
        return maxElectionTimeout;
    }

    public int getMinElectionTimeout() {
        return minElectionTimeout;
    }

    public int getHeartbeatPeriod() {
        return heartbeatPeriod;
    }

    public int getPeerTimeout() {
        return peerTimeout;
    }

    public int getSendFailureBackoff() {
        return sendFailureBackoff;
    }

    public int getMaxPublishingLogEntryCount() {
        return maxPublishingLogEntryCount;
    }

    public int getMinStateTransferGapLogEntryCount() {
        return minStateTransferGapLogEntryCount;
    }

    public int getClientSessionTimeout() {
        return clientSessionTimeout;
    }

    public int getJoinGroupPeriod() {
        return joinGroupPeriod;
    }

    public int getLeaveGroupPeriod() {
        return leaveGroupPeriod;
    }

    public int getLeaveGroupAttemptCount() {
        return leaveGroupAttemptCount;
    }

    public int getAutoLeaveGroupTimeout() {
        return autoLeaveGroupTimeout;
    }

    public int getMaxValueSize() {
        return maxValueSize;
    }

    public int getLockQueueCapacity() {
        return lockQueueCapacity;
    }

    public int getUnlockQueueCapacity() {
        return unlockQueueCapacity;
    }

    public long getCommitPeriod() {
        return commitPeriod;
    }

    public long getSubscriptionTimeout() {
        return subscriptionTimeout;
    }

    public IServerChannelFactory createFactory() {
        return new ServerChannelFactory(this);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof ServerChannelFactoryConfiguration))
            return false;

        ServerChannelFactoryConfiguration configuration = (ServerChannelFactoryConfiguration) o;
        return timerPeriod == configuration.timerPeriod && maxElectionTimeout == configuration.maxElectionTimeout &&
            minElectionTimeout == configuration.minElectionTimeout &&
            heartbeatPeriod == configuration.heartbeatPeriod && peerTimeout == configuration.peerTimeout &&
            sendFailureBackoff == configuration.sendFailureBackoff &&
            maxPublishingLogEntryCount == configuration.maxPublishingLogEntryCount && minStateTransferGapLogEntryCount == configuration.minStateTransferGapLogEntryCount &&
            clientSessionTimeout == configuration.clientSessionTimeout && joinGroupPeriod == configuration.joinGroupPeriod &&
            leaveGroupPeriod == configuration.leaveGroupPeriod && leaveGroupAttemptCount == configuration.leaveGroupAttemptCount &&
            autoLeaveGroupTimeout == configuration.autoLeaveGroupTimeout && maxValueSize == configuration.maxValueSize &&
            lockQueueCapacity == configuration.lockQueueCapacity && unlockQueueCapacity == configuration.unlockQueueCapacity &&
            commitPeriod == configuration.commitPeriod && subscriptionTimeout == configuration.subscriptionTimeout;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(timerPeriod, maxElectionTimeout, minElectionTimeout, heartbeatPeriod, peerTimeout, sendFailureBackoff,
            maxPublishingLogEntryCount, minStateTransferGapLogEntryCount, clientSessionTimeout, joinGroupPeriod, leaveGroupPeriod,
            leaveGroupAttemptCount, autoLeaveGroupTimeout, maxValueSize, lockQueueCapacity, unlockQueueCapacity, commitPeriod,
            subscriptionTimeout);
    }
}
