package com.artos.impl.core.server.client;

import com.artos.impl.core.message.FollowerRequest;
import com.exametrika.common.utils.ILifecycle;

public interface IFollowerMessageSender extends ILifecycle {
    void send(FollowerRequest request);
}
