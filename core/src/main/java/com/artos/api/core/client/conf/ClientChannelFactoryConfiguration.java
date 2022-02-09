package com.artos.api.core.client.conf;

import com.artos.api.core.client.IClientChannelFactory;
import com.artos.impl.core.client.ClientChannelFactory;
import com.exametrika.common.config.Configuration;
import com.exametrika.common.utils.Objects;

public class ClientChannelFactoryConfiguration extends Configuration {
    private final long heartbeatPeriod;
    private final long updateConfigurationPeriod;
    private final int lockQueueCapacity;
    private final int unlockQueueCapacity;
    private final long timerPeriod;
    private final long serverTimeout;
    private final int maxValueSize;
    private final int maxBatchSize;
    private final long resendQueryPeriod;
    private final long resubscriptionPeriod;

    public ClientChannelFactoryConfiguration(long heartbeatPeriod, long updateConfigurationPeriod,
                                             int lockQueueCapacity, int unlockQueueCapacity, long timerPeriod,
                                             long serverTimeout, int maxValueSize, int maxBatchSize, long resendQueryPeriod,
                                             long resubscriptionPeriod) {
        this.heartbeatPeriod = heartbeatPeriod;
        this.updateConfigurationPeriod = updateConfigurationPeriod;
        this.lockQueueCapacity = lockQueueCapacity;
        this.unlockQueueCapacity = unlockQueueCapacity;
        this.timerPeriod = timerPeriod;
        this.serverTimeout = serverTimeout;
        this.maxValueSize = maxValueSize;
        this.maxBatchSize = maxBatchSize;
        this.resendQueryPeriod = resendQueryPeriod;
        this.resubscriptionPeriod = resubscriptionPeriod;
    }

    public long getHeartbeatPeriod() {
        return heartbeatPeriod;
    }

    public long getUpdateConfigurationPeriod() {
        return updateConfigurationPeriod;
    }

    public int getLockQueueCapacity() {
        return lockQueueCapacity;
    }

    public int getUnlockQueueCapacity() {
        return unlockQueueCapacity;
    }

    public long getTimerPeriod() {
        return timerPeriod;
    }

    public long getServerTimeout() {
        return serverTimeout;
    }

    public int getMaxValueSize() {
        return maxValueSize;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public long getResendQueryPeriod() {
        return resendQueryPeriod;
    }

    public long getResubscriptionPeriod() {
        return resubscriptionPeriod;
    }

    public IClientChannelFactory createFactory() {
       return new ClientChannelFactory(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ClientChannelFactoryConfiguration))
            return false;

        ClientChannelFactoryConfiguration configuration = (ClientChannelFactoryConfiguration) o;
        return heartbeatPeriod == configuration.heartbeatPeriod &&
            updateConfigurationPeriod == configuration.updateConfigurationPeriod &&
            lockQueueCapacity == configuration.lockQueueCapacity &&
            unlockQueueCapacity == configuration.unlockQueueCapacity &&
            timerPeriod == configuration.timerPeriod &&
            serverTimeout == configuration.serverTimeout &&
            maxValueSize == configuration.maxValueSize &&
            maxBatchSize == configuration.maxBatchSize &&
            resendQueryPeriod == configuration.resendQueryPeriod &&
            resubscriptionPeriod == configuration.resubscriptionPeriod;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(heartbeatPeriod, updateConfigurationPeriod, lockQueueCapacity,
            unlockQueueCapacity, timerPeriod, serverTimeout, maxValueSize, maxBatchSize, resendQueryPeriod, resubscriptionPeriod);
    }
}
