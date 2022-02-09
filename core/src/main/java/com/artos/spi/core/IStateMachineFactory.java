package com.artos.spi.core;

public interface IStateMachineFactory {
    IStateMachine createStateMachine(IGroupChannel groupChannel);
}
