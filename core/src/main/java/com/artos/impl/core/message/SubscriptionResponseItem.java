package com.artos.impl.core.message;

import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;

import java.util.UUID;

public class SubscriptionResponseItem {
    private final UUID subscriptionId;
    private final ByteArray value;

    public SubscriptionResponseItem(UUID subscriptionId, ByteArray value) {
        Assert.notNull(subscriptionId);
        Assert.notNull(value);

        this.subscriptionId = subscriptionId;
        this.value = value;
    }

    public UUID getSubscriptionId() {
        return subscriptionId;
    }

    public ByteArray getValue() {
        return value;
    }

    @Override
    public String toString() {
        return subscriptionId.toString();
    }
}
