/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.api.core.server.conf;

public class ServerChannelFactoryConfigurationBuilder {
    private int timerPeriod = 50;
    private int maxElectionTimeout = 2000;
    private int minElectionTimeout = 1000;
    private int heartbeatPeriod = 150;
    private int peerTimeout = 2000;
    private int sendFailureBackoff = 50;
    private int maxPublishingLogEntryCount = 1000;
    private int minStateTransferGapLogEntryCount = 1000000;
    private int clientSessionTimeout = 600000;
    private int joinGroupPeriod = 1000;
    private int leaveGroupPeriod = 1000;
    private int leaveGroupAttemptCount = 10;
    private int autoLeaveGroupTimeout = 0;
    private int maxValueSize = 1000000;
    private int lockQueueCapacity = 200;
    private int unlockQueueCapacity = 140;
    private long commitPeriod = 100;
    private long subscriptionTimeout = 600000;

    public int getTimerPeriod() {
        return timerPeriod;
    }

    public ServerChannelFactoryConfigurationBuilder setTimerPeriod(int timerPeriod) {
        this.timerPeriod = timerPeriod;
        return this;
    }

    public int getMaxValueSize() {
        return maxValueSize;
    }

    public ServerChannelFactoryConfigurationBuilder setMaxValueSize(int maxValueSize) {
        this.maxValueSize = maxValueSize;
        return this;
    }

    public int getLockQueueCapacity() {
        return lockQueueCapacity;
    }

    public ServerChannelFactoryConfigurationBuilder setLockQueueCapacity(int lockQueueCapacity) {
        this.lockQueueCapacity = lockQueueCapacity;
        return this;
    }

    public int getUnlockQueueCapacity() {
        return unlockQueueCapacity;
    }

    public ServerChannelFactoryConfigurationBuilder setUnlockQueueCapacity(int unlockQueueCapacity) {
        this.unlockQueueCapacity = unlockQueueCapacity;
        return this;
    }

    public long getCommitPeriod() {
        return commitPeriod;
    }

    public ServerChannelFactoryConfigurationBuilder setCommitPeriod(long commitPeriod) {
        this.commitPeriod = commitPeriod;
        return this;
    }

    /**
     * The maximum log entries could be attached to an publish call
     * @param maxPublishingLogEntryCount size limit for publish call
     * @return self
     */
    public ServerChannelFactoryConfigurationBuilder setMaxPublishingLogEntryCount(int maxPublishingLogEntryCount) {
        this.maxPublishingLogEntryCount = maxPublishingLogEntryCount;
        return this;
    }

    /**
     * Election timeout upper bound in milliseconds
     * @param electionTimeoutUpper election timeout upper value
     * @return self
     */
    public ServerChannelFactoryConfigurationBuilder setMaxElectionTimeout(int electionTimeoutUpper) {
        this.maxElectionTimeout = electionTimeoutUpper;
        return this;
    }

    /**
     * Election timeout lower bound in milliseconds
     * @param electionTimeoutLower election timeout lower value
     * @return self
     */
    public ServerChannelFactoryConfigurationBuilder setMinElectionTimeout(int electionTimeoutLower) {
        this.minElectionTimeout = electionTimeoutLower;
        return this;
    }

    /**
     * heartbeat period in milliseconds
     * @param heartbeatPeriod heartbeat period
     * @return self
     */
    public ServerChannelFactoryConfigurationBuilder setHeartbeatPeriod(int heartbeatPeriod) {
        this.heartbeatPeriod = heartbeatPeriod;
        return this;
    }

    /**
     * Send failure backoff in milliseconds
     * @param sendFailureBackoff send failure back off
     * @return self
     */
    public ServerChannelFactoryConfigurationBuilder setSendFailureBackoff(int sendFailureBackoff) {
        this.sendFailureBackoff = sendFailureBackoff;
        return this;
    }

    /**
     * Minimum gap between last log entry of leader and last log entry of follower which causes
     * state synchronization using dedicated state transfer process instead of ordinary replication of log entries
     * @param minStateTransferGapLogEntryCount gap between last log entry of leader and last log entry of follower to use state transfer
     * @return self
     */
    public ServerChannelFactoryConfigurationBuilder setMinStateTransferGapLogEntryCount(int minStateTransferGapLogEntryCount) {
        this.minStateTransferGapLogEntryCount = minStateTransferGapLogEntryCount;
        return this;
    }

    /**
     * Timeout for client sessions on server.
     * @param clientSessionTimeout client session timeout in milliseconds
     * @return self
     */
    public ServerChannelFactoryConfigurationBuilder setClientSessionTimeout(int clientSessionTimeout) {
        this.clientSessionTimeout = clientSessionTimeout;
        return this;
    }

    public ServerChannelFactoryConfigurationBuilder setJoinGroupPeriod(int joinGroupPeriod) {
        this.joinGroupPeriod = joinGroupPeriod;
        return this;
    }

    public ServerChannelFactoryConfigurationBuilder setLeaveGroupPeriod(int leaveGroupPeriod) {
        this.leaveGroupPeriod = leaveGroupPeriod;
        return this;
    }

    public ServerChannelFactoryConfigurationBuilder setLeaveGroupAttemptCount(int leaveGroupAttemptCount) {
        this.leaveGroupAttemptCount = leaveGroupAttemptCount;
        return this;
    }

    public ServerChannelFactoryConfigurationBuilder setAutoLeaveGroupTimeout(int leaveGroupTimeout) {
        this.autoLeaveGroupTimeout = leaveGroupTimeout;
        return this;
    }

    public ServerChannelFactoryConfigurationBuilder setSubscriptionTimeout(long subscriptionTimeout) {
        this.subscriptionTimeout = subscriptionTimeout;
        return this;
    }

    /**
     * Upper value for election timeout
     * @return upper of election timeout in milliseconds
     */
    public int getMaxElectionTimeout() {
        return maxElectionTimeout;
    }

    /**
     * Lower value for election timeout
     * @return lower of election timeout in milliseconds
     */
    public int getMinElectionTimeout() {
        return minElectionTimeout;
    }

    /**
     * Heartbeat period for each peer
     * @return heartbeat period in milliseconds
     */
    public long getHeartbeatPeriod() {
        return heartbeatPeriod;
    }

    public int getPeerTimeout() {
        return peerTimeout;
    }

    public void setPeerTimeout(int peerTimeout) {
        this.peerTimeout = peerTimeout;
    }

    /**
     * Send backoff for peers that failed to be connected
     * @return send backoff in milliseconds
     */
    public int getSendFailureBackoff() {
        return sendFailureBackoff;
    }

    /**
     * The maximum log entries in an publish request
     * @return maximum log entries
     */
    public int getMaxPublishingLogEntryCount() {
        return this.maxPublishingLogEntryCount;
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

    public long getSubscriptionTimeout() {
        return subscriptionTimeout;
    }

    public ServerChannelFactoryConfiguration toConfiguration() {
        return new ServerChannelFactoryConfiguration(timerPeriod, maxElectionTimeout, minElectionTimeout, heartbeatPeriod,
            peerTimeout, sendFailureBackoff, maxPublishingLogEntryCount, minStateTransferGapLogEntryCount,
            clientSessionTimeout, joinGroupPeriod, leaveGroupPeriod,
            leaveGroupAttemptCount, autoLeaveGroupTimeout, maxValueSize, lockQueueCapacity, unlockQueueCapacity, commitPeriod,
            subscriptionTimeout);
    }
}
