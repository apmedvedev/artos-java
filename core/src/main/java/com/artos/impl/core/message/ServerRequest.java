/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

import java.util.Set;
import java.util.UUID;

public abstract class ServerRequest extends Request {

    public ServerRequest(UUID groupId, UUID source, Set<MessageFlags> flags) {
        super(groupId, source, flags);
    }

    @Override
    protected String doToString() {
        return  "";
    }
}
