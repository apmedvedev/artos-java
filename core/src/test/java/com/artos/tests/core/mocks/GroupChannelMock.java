package com.artos.tests.core.mocks;

import com.artos.api.core.server.IMembershipListener;
import com.artos.api.core.server.conf.GroupConfiguration;
import com.artos.api.core.server.conf.ServerConfiguration;
import com.artos.spi.core.IGroupChannel;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.ICompletionHandler;

import java.util.UUID;

public class GroupChannelMock implements IGroupChannel {
    @Override
    public void publish(ByteArray value, ICompletionHandler<ByteArray> commitHandler) {
    }

    @Override
    public ServerConfiguration getLocalServer() {
        return null;
    }

    @Override
    public GroupConfiguration getGroup() {
        return null;
    }

    @Override
    public UUID getLeader() {
        return null;
    }

    @Override
    public void addMembershipListener(IMembershipListener listener) {
    }

    @Override
    public void removeMembershipListener(IMembershipListener listener) {
    }

    @Override
    public void removeAllMembershipListeners() {
    }
}
