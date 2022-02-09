package com.artos.api.core.client.conf;

public class ClientChannelFactoryConfigurationBuilder {
    private long heartbeatPeriod = 500;
    private long updateConfigurationPeriod = 2000;
    private int lockQueueCapacity = 200;
    private int unlockQueueCapacity = 140;
    private long timerPeriod = 50;
    private long serverTimeout = 2000;
    private int maxValueSize = 1000000;
    private int maxBatchSize = 10000000;
    private int resendQueryPeriod = 600000;
    private int resubscriptionPeriod = 60000;

    public long getHeartbeatPeriod() {
        return heartbeatPeriod;
    }

    public ClientChannelFactoryConfigurationBuilder setHeartbeatPeriod(long heartbeatPeriod) {
        this.heartbeatPeriod = heartbeatPeriod;
        return this;
    }

    public long getUpdateConfigurationPeriod() {
        return updateConfigurationPeriod;
    }

    public ClientChannelFactoryConfigurationBuilder setUpdateConfigurationPeriod(long updateConfigurationPeriod) {
        this.updateConfigurationPeriod = updateConfigurationPeriod;
        return this;
    }

    public int getLockQueueCapacity() {
        return lockQueueCapacity;
    }

    public ClientChannelFactoryConfigurationBuilder setLockQueueCapacity(int lockQueueCapacity) {
        this.lockQueueCapacity = lockQueueCapacity;
        return this;
    }

    public int getUnlockQueueCapacity() {
        return unlockQueueCapacity;
    }

    public ClientChannelFactoryConfigurationBuilder setUnlockQueueCapacity(int unlockQueueCapacity) {
        this.unlockQueueCapacity = unlockQueueCapacity;
        return this;
    }

    public long getTimerPeriod() {
        return timerPeriod;
    }

    public ClientChannelFactoryConfigurationBuilder setTimerPeriod(long timerPeriod) {
        this.timerPeriod = timerPeriod;
        return this;
    }

    public long getServerTimeout() {
        return serverTimeout;
    }

    public ClientChannelFactoryConfigurationBuilder setServerTimeout(long serverTimeout) {
        this.serverTimeout = serverTimeout;
        return this;
    }

    public int getMaxValueSize() {
        return maxValueSize;
    }

    public ClientChannelFactoryConfigurationBuilder setMaxValueSize(int maxValueSize) {
        this.maxValueSize = maxValueSize;
        return this;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public ClientChannelFactoryConfigurationBuilder setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    public int getResendQueryPeriod() {
        return resendQueryPeriod;
    }

    public ClientChannelFactoryConfigurationBuilder setResendQueryPeriod(int resendQueryPeriod) {
        this.resendQueryPeriod = resendQueryPeriod;
        return this;
    }

    public int getResubscriptionPeriod() {
        return resubscriptionPeriod;
    }

    public ClientChannelFactoryConfigurationBuilder setResubscriptionPeriod(int resubscriptionPeriod) {
        this.resubscriptionPeriod = resubscriptionPeriod;
        return this;
    }

    public ClientChannelFactoryConfiguration toConfiguration() {
        return new ClientChannelFactoryConfiguration(heartbeatPeriod, updateConfigurationPeriod, lockQueueCapacity,
            unlockQueueCapacity, timerPeriod, serverTimeout, maxValueSize, maxBatchSize, resendQueryPeriod, resubscriptionPeriod);
    }
}
