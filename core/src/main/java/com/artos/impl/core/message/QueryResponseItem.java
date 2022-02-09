package com.artos.impl.core.message;

import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;

public class QueryResponseItem {
    private final long messageId;
    private final ByteArray value;

    public QueryResponseItem(long messageId, ByteArray value) {
        Assert.notNull(value);

        this.messageId = messageId;
        this.value = value;
    }

    public long getMessageId() {
        return messageId;
    }

    public ByteArray getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "" + messageId;
    }
}
