package com.artos.impl.core.message;

import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;

public class QueryRequestItem {
    private final long lastPublishedMessageId;
    private final long messageId;
    private final ByteArray value;

    public QueryRequestItem(long lastPublishedMessageId, long messageId, ByteArray value) {
        Assert.notNull(value);

        this.lastPublishedMessageId = lastPublishedMessageId;
        this.messageId = messageId;
        this.value = value;
    }

    public long getLastPublishedMessageId() {
        return lastPublishedMessageId;
    }

    public long getMessageId() {
        return messageId;
    }

    public ByteArray getValue() {
        return value;
    }

    @Override
    public String toString() {
        return lastPublishedMessageId + ":" + messageId;
    }
}
