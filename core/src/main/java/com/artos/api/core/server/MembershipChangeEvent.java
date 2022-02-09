package com.artos.api.core.server;

import com.artos.api.core.server.conf.GroupConfiguration;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.utils.Assert;

public class MembershipChangeEvent {
    private static final IMessages messages = Messages.get(IMessages.class);
    private final GroupConfiguration oldMembership;
    private final GroupConfiguration newMembership;
    private final MembershipChange membershipChange;

    public MembershipChangeEvent(GroupConfiguration oldMembership, GroupConfiguration newMembership, MembershipChange membershipChange) {
        Assert.notNull(oldMembership);
        Assert.notNull(newMembership);
        Assert.notNull(membershipChange);

        this.oldMembership = oldMembership;
        this.newMembership = newMembership;
        this.membershipChange = membershipChange;
    }

    public GroupConfiguration getOldMembership() {
        return oldMembership;
    }

    public GroupConfiguration getNewMembership() {
        return newMembership;
    }

    public MembershipChange getMembershipChange() {
        return membershipChange;
    }

    @Override
    public String toString() {
        return messages.toString(oldMembership, newMembership, membershipChange).toString();
    }

    private interface IMessages {
        @DefaultMessage("old: {0}\nnew: {1}\nchange: {2}")
        ILocalizedMessage toString(GroupConfiguration oldMembership, GroupConfiguration newMembership, MembershipChange membershipChange);
    }
}
