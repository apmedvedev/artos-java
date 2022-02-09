package com.artos.inttests.core.sim.model;

import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public interface ISimPublishChannel {
    void publish(ByteArray value, ICompletionHandler<ByteArray> commitHandler);
}
