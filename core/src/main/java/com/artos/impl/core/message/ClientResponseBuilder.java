/**
 * Copyright 2020 Andrey Medvedev. All rights reserved.
 */

package com.artos.impl.core.message;

public class ClientResponseBuilder extends ResponseBuilder {
    private boolean accepted;

    public boolean isAccepted() {
        return accepted;
    }

    public void setAccepted(boolean accepted) {
        this.accepted = accepted;
    }
}
