package com.artos.spi.core;

import com.artos.api.core.server.IMembershipService;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

public interface IGroupChannel extends IMembershipService {
    void publish(ByteArray value, ICompletionHandler<ByteArray> commitHandler);
}
