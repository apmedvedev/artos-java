package com.artos.impl.core.server.utils;

import com.artos.impl.core.message.AppendEntriesRequest;
import com.artos.impl.core.message.AppendEntriesRequestBuilder;
import com.artos.impl.core.message.AppendEntriesResponse;
import com.artos.impl.core.message.AppendEntriesResponseBuilder;
import com.artos.impl.core.message.ClientRequest;
import com.artos.impl.core.message.ClientRequestBuilder;
import com.artos.impl.core.message.ClientResponse;
import com.artos.impl.core.message.ClientResponseBuilder;
import com.artos.impl.core.message.ClientSessionTransientState;
import com.artos.impl.core.message.FollowerRequest;
import com.artos.impl.core.message.FollowerRequestBuilder;
import com.artos.impl.core.message.FollowerResponse;
import com.artos.impl.core.message.FollowerResponseBuilder;
import com.artos.impl.core.message.GroupTransientState;
import com.artos.impl.core.message.GroupTransientStateBuilder;
import com.artos.impl.core.message.JoinGroupRequest;
import com.artos.impl.core.message.JoinGroupRequestBuilder;
import com.artos.impl.core.message.JoinGroupResponse;
import com.artos.impl.core.message.JoinGroupResponseBuilder;
import com.artos.impl.core.message.LeaveGroupRequest;
import com.artos.impl.core.message.LeaveGroupRequestBuilder;
import com.artos.impl.core.message.LeaveGroupResponse;
import com.artos.impl.core.message.LeaveGroupResponseBuilder;
import com.artos.impl.core.message.Message;
import com.artos.impl.core.message.MessageBuilder;
import com.artos.impl.core.message.MessageFlags;
import com.artos.impl.core.message.PublishRequest;
import com.artos.impl.core.message.PublishRequestBuilder;
import com.artos.impl.core.message.PublishResponse;
import com.artos.impl.core.message.PublishResponseBuilder;
import com.artos.impl.core.message.QueryRequest;
import com.artos.impl.core.message.QueryRequestBuilder;
import com.artos.impl.core.message.QueryRequestItem;
import com.artos.impl.core.message.QueryResponse;
import com.artos.impl.core.message.QueryResponseBuilder;
import com.artos.impl.core.message.QueryResponseItem;
import com.artos.impl.core.message.Request;
import com.artos.impl.core.message.RequestBuilder;
import com.artos.impl.core.message.Response;
import com.artos.impl.core.message.ResponseBuilder;
import com.artos.impl.core.message.ServerRequest;
import com.artos.impl.core.message.ServerRequestBuilder;
import com.artos.impl.core.message.ServerResponse;
import com.artos.impl.core.message.ServerResponseBuilder;
import com.artos.impl.core.message.SubscriptionRequest;
import com.artos.impl.core.message.SubscriptionRequestBuilder;
import com.artos.impl.core.message.SubscriptionRequestItem;
import com.artos.impl.core.message.SubscriptionResponse;
import com.artos.impl.core.message.SubscriptionResponseBuilder;
import com.artos.impl.core.message.SubscriptionResponseItem;
import com.artos.impl.core.message.VoteRequest;
import com.artos.impl.core.message.VoteRequestBuilder;
import com.artos.impl.core.message.VoteResponse;
import com.artos.impl.core.message.VoteResponseBuilder;
import com.artos.spi.core.LogEntry;
import com.exametrika.common.io.impl.ByteInputStream;
import com.exametrika.common.io.impl.ByteOutputStream;
import com.exametrika.common.io.impl.DataDeserialization;
import com.exametrika.common.io.impl.DataSerialization;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.ByteArray;
import com.exametrika.common.utils.Serializers;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class MessageUtils {
    public static ByteArray write(Message message) {
        ByteOutputStream out = new ByteOutputStream();
        DataSerialization serialization = new DataSerialization(out);

        if (message instanceof AppendEntriesRequest) {
            serialization.writeByte((byte)1);
            writeAppendEntriesRequest(serialization, (AppendEntriesRequest) message);
        } else if (message instanceof AppendEntriesResponse) {
            serialization.writeByte((byte)2);
            writeAppendEntriesResponse(serialization, (AppendEntriesResponse) message);
        } else if (message instanceof PublishRequest) {
            serialization.writeByte((byte)3);
            writePublishRequest(serialization, (PublishRequest) message);
        } else if (message instanceof PublishResponse) {
            serialization.writeByte((byte)4);
            writePublishResponse(serialization, (PublishResponse) message);
        } else if (message instanceof JoinGroupRequest) {
            serialization.writeByte((byte)5);
            writeJoinGroupRequest(serialization, (JoinGroupRequest) message);
        } else if (message instanceof JoinGroupResponse) {
            serialization.writeByte((byte)6);
            writeJoinGroupResponse(serialization, (JoinGroupResponse) message);
        } else if (message instanceof LeaveGroupRequest) {
            serialization.writeByte((byte)7);
            writeLeaveGroupRequest(serialization, (LeaveGroupRequest) message);
        } else if (message instanceof LeaveGroupResponse) {
            serialization.writeByte((byte)8);
            writeLeaveGroupResponse(serialization, (LeaveGroupResponse) message);
        } else if (message instanceof VoteRequest) {
            serialization.writeByte((byte)9);
            writeVoteRequest(serialization, (VoteRequest) message);
        } else if (message instanceof VoteResponse) {
            serialization.writeByte((byte)10);
            writeVoteResponse(serialization, (VoteResponse) message);
        } else if (message instanceof QueryRequest) {
            serialization.writeByte((byte)11);
            writeQueryRequest(serialization, (QueryRequest) message);
        } else if (message instanceof QueryResponse) {
            serialization.writeByte((byte)12);
            writeQueryResponse(serialization, (QueryResponse) message);
        } else if (message instanceof SubscriptionRequest) {
            serialization.writeByte((byte)13);
            writeSubscriptionRequest(serialization, (SubscriptionRequest) message);
        } else if (message instanceof SubscriptionResponse) {
            serialization.writeByte((byte)14);
            writeSubscriptionResponse(serialization, (SubscriptionResponse) message);
        } else {
            Assert.error();
        }

        return new ByteArray(out.getBuffer(), 0, out.getLength());
    }

    public static Message read(ByteArray buffer) {
        ByteInputStream in = new ByteInputStream(buffer.getBuffer(), buffer.getOffset(), buffer.getLength());
        DataDeserialization deserialization = new DataDeserialization(in);
        byte type = deserialization.readByte();
        switch (type) {
            case 1:
                return readAppendEntriesRequest(deserialization);
            case 2:
                return readAppendEntriesResponse(deserialization);
            case 3:
                return readPublishRequest(deserialization);
            case 4:
                return readPublishResponse(deserialization);
            case 5:
                return readJoinGroupRequest(deserialization);
            case 6:
                return readJoinGroupResponse(deserialization);
            case 7:
                return readLeaveGroupRequest(deserialization);
            case 8:
                return readLeaveGroupResponse(deserialization);
            case 9:
                return readVoteRequest(deserialization);
            case 10:
                return readVoteResponse(deserialization);
            case 11:
                return readQueryRequest(deserialization);
            case 12:
                return readQueryResponse(deserialization);
            case 13:
                return readSubscriptionRequest(deserialization);
            case 14:
                return readSubscriptionResponse(deserialization);
            default:
                return Assert.error();
        }
    }

    private static void writeAppendEntriesRequest(DataSerialization serialization, AppendEntriesRequest message) {
        writeServerRequest(serialization, message);

        serialization.writeLong(message.getTerm());
        serialization.writeLong(message.getLastLogTerm());
        serialization.writeLong(message.getLastLogIndex());
        serialization.writeLong(message.getCommitIndex());

        if (message.getLogEntries() != null) {
            serialization.writeBoolean(true);
            serialization.writeInt(message.getLogEntries().size());
            for (LogEntry logEntry : message.getLogEntries())
                Utils.writeLogEntry(serialization, logEntry);
        } else {
            serialization.writeBoolean(false);
        }
    }

    private static Message readAppendEntriesRequest(DataDeserialization deserialization) {
        AppendEntriesRequestBuilder builder = new AppendEntriesRequestBuilder();
        readServerRequest(deserialization, builder);

        builder.setTerm(deserialization.readLong());
        builder.setLastLogTerm(deserialization.readLong());
        builder.setLastLogIndex(deserialization.readLong());
        builder.setCommitIndex(deserialization.readLong());

        if (deserialization.readBoolean()) {
            int count = deserialization.readInt();

            List<LogEntry> logEntries = new ArrayList<>(count);
            for (int i = 0; i < count; i++)
                logEntries.add(Utils.readLogEntry(deserialization));

            builder.setLogEntries(logEntries);
        }

        return builder.toMessage();
    }

    private static void writeAppendEntriesResponse(DataSerialization serialization, AppendEntriesResponse message) {
        writeServerResponse(serialization, message);

        serialization.writeLong(message.getTerm());
        serialization.writeLong(message.getNextIndex());
    }

    private static Message readAppendEntriesResponse(DataDeserialization deserialization) {
        AppendEntriesResponseBuilder builder = new AppendEntriesResponseBuilder();
        readServerResponse(deserialization, builder);

        builder.setTerm(deserialization.readLong());
        builder.setNextIndex(deserialization.readLong());

        return builder.toMessage();
    }

    private static void writePublishRequest(DataSerialization serialization, PublishRequest message) {
        writeClientRequest(serialization, message);

        if (message.getLogEntries() != null) {
            serialization.writeBoolean(true);
            serialization.writeInt(message.getLogEntries().size());
            for (LogEntry logEntry : message.getLogEntries())
                Utils.writeLogEntry(serialization, logEntry);
        } else {
            serialization.writeBoolean(false);
        }
    }

    private static Message readPublishRequest(DataDeserialization deserialization) {
        PublishRequestBuilder builder = new PublishRequestBuilder();

        readClientRequest(deserialization, builder);

        if (deserialization.readBoolean()) {
            int count = deserialization.readInt();

            List<LogEntry> logEntries = new ArrayList<>(count);
            for (int i = 0; i < count; i++)
                logEntries.add(Utils.readLogEntry(deserialization));

            builder.setLogEntries(logEntries);
        }

        return builder.toMessage();
    }

    private static void writePublishResponse(DataSerialization serialization, PublishResponse message) {
        writeClientResponse(serialization, message);

        if (message.getLeaderId() != null) {
            serialization.writeBoolean(true);
            Serializers.writeUUID(serialization, message.getLeaderId());
        } else {
            serialization.writeBoolean(false);
        }

        serialization.writeLong(message.getLastReceivedMessageId());
        serialization.writeLong(message.getLastCommittedMessageId());

        if (message.getConfiguration() != null) {
            serialization.writeBoolean(true);
            Utils.writeGroupConfiguration(serialization, message.getConfiguration());
        } else {
            serialization.writeBoolean(false);
        }
    }

    private static Message readPublishResponse(DataDeserialization deserialization) {
        PublishResponseBuilder builder = new PublishResponseBuilder();
        readClientResponse(deserialization, builder);

        if (deserialization.readBoolean())
            builder.setLeaderId(Serializers.readUUID(deserialization));

        builder.setLastReceivedMessageId(deserialization.readLong());
        builder.setLastCommittedMessageId(deserialization.readLong());

        if (deserialization.readBoolean())
            builder.setConfiguration(Utils.readGroupConfiguration(deserialization));

        return builder.toMessage();
    }

    private static void writeJoinGroupRequest(DataSerialization serialization, JoinGroupRequest message) {
        writeFollowerRequest(serialization, message);

        serialization.writeLong(message.getLastLogTerm());
        serialization.writeLong(message.getLastLogIndex());
        Utils.writeServerConfiguration(serialization, message.getConfiguration());
    }

    private static Message readJoinGroupRequest(DataDeserialization deserialization) {
        JoinGroupRequestBuilder builder = new JoinGroupRequestBuilder();
        readFollowerRequest(deserialization, builder);

        builder.setLastLogTerm(deserialization.readLong());
        builder.setLastLogIndex(deserialization.readLong());
        builder.setConfiguration(Utils.readServerConfiguration(deserialization));

        return builder.toMessage();
    }

    private static void writeJoinGroupResponse(DataSerialization serialization, JoinGroupResponse message) {
        writeFollowerResponse(serialization, message);

        if (message.getStateTransferConfiguration() != null) {
            serialization.writeBoolean(true);
            Utils.writeStateTransferConfiguration(serialization, message.getStateTransferConfiguration());
        } else {
            serialization.writeBoolean(false);
        }
    }

    private static Message readJoinGroupResponse(DataDeserialization deserialization) {
        JoinGroupResponseBuilder builder = new JoinGroupResponseBuilder();
        readFollowerResponse(deserialization, builder);

        if (deserialization.readBoolean())
            builder.setStateTransferConfiguration(Utils.readStateTransferConfiguration(deserialization));

        return builder.toMessage();
    }

    private static void writeLeaveGroupRequest(DataSerialization serialization, LeaveGroupRequest message) {
        writeFollowerRequest(serialization, message);
    }

    private static Message readLeaveGroupRequest(DataDeserialization deserialization) {
        LeaveGroupRequestBuilder builder = new LeaveGroupRequestBuilder();
        readFollowerRequest(deserialization, builder);

        return builder.toMessage();
    }

    private static void writeLeaveGroupResponse(DataSerialization serialization, LeaveGroupResponse message) {
        writeFollowerResponse(serialization, message);

        if (message.getConfiguration() != null) {
            serialization.writeBoolean(true);
            Utils.writeGroupConfiguration(serialization, message.getConfiguration());
        } else {
            serialization.writeBoolean(false);
        }
    }

    private static Message readLeaveGroupResponse(DataDeserialization deserialization) {
        LeaveGroupResponseBuilder builder = new LeaveGroupResponseBuilder();
        readFollowerResponse(deserialization, builder);

        if (deserialization.readBoolean())
            builder.setConfiguration(Utils.readGroupConfiguration(deserialization));

        return builder.toMessage();
    }

    private static void writeVoteRequest(DataSerialization serialization, VoteRequest message) {
        writeServerRequest(serialization, message);

        serialization.writeLong(message.getTerm());
        serialization.writeLong(message.getLastLogTerm());
        serialization.writeLong(message.getLastLogIndex());
    }

    private static Message readVoteRequest(DataDeserialization deserialization) {
        VoteRequestBuilder builder = new VoteRequestBuilder();
        readServerRequest(deserialization, builder);

        builder.setTerm(deserialization.readLong());
        builder.setLastLogTerm(deserialization.readLong());
        builder.setLastLogIndex(deserialization.readLong());

        return builder.toMessage();
    }

    private static void writeVoteResponse(DataSerialization serialization, VoteResponse message) {
        writeServerResponse(serialization, message);

        if (message.getTransientState() != null) {
            serialization.writeBoolean(true);
            writeGroupTransientState(serialization, message.getTransientState());
        } else {
            serialization.writeBoolean(false);
        }

        serialization.writeLong(message.getTerm());
    }

    private static Message readVoteResponse(DataDeserialization deserialization) {
        VoteResponseBuilder builder = new VoteResponseBuilder();
        readServerResponse(deserialization, builder);

        if (deserialization.readBoolean())
            builder.setTransientState(readGroupTransientState(deserialization));

        builder.setTerm(deserialization.readLong());

        return builder.toMessage();
    }

    private static void writeGroupTransientState(DataSerialization serialization, GroupTransientState transientState) {
        serialization.writeLong(transientState.getCommitIndex());
        serialization.writeInt(transientState.getClientSessions().size());
        for (ClientSessionTransientState session : transientState.getClientSessions()) {
            Serializers.writeUUID(serialization, session.getClientId());
            serialization.writeLong(session.getLastCommittedMessageId());
        }
    }

    private static GroupTransientState readGroupTransientState(DataDeserialization deserialization) {
        GroupTransientStateBuilder builder = new GroupTransientStateBuilder();

        long commitIndex = deserialization.readLong();
        builder.setCommitIndex(commitIndex);

        int count = deserialization.readInt();
        List<ClientSessionTransientState> clientSessions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            UUID clientId = Serializers.readUUID(deserialization);
            long lastCommittedMessageId = deserialization.readLong();

            clientSessions.add(new ClientSessionTransientState(clientId, lastCommittedMessageId));
        }

        builder.setClientSessions(clientSessions);

        return builder.toState();
    }

    private static void writeQueryRequest(DataSerialization serialization, QueryRequest message) {
        writeClientRequest(serialization, message);

        if (message.getQueries() != null) {
            serialization.writeBoolean(true);
            serialization.writeInt(message.getQueries().size());
            for (QueryRequestItem query : message.getQueries())
                writeQueryRequestItem(serialization, query);
        } else {
            serialization.writeBoolean(false);
        }
    }

    private static Message readQueryRequest(DataDeserialization deserialization) {
        QueryRequestBuilder builder = new QueryRequestBuilder();

        readClientRequest(deserialization, builder);

        if (deserialization.readBoolean()) {
            int count = deserialization.readInt();

            List<QueryRequestItem> queries = new ArrayList<>(count);
            for (int i = 0; i < count; i++)
                queries.add(readQueryRequestItem(deserialization));

            builder.setQueries(queries);
        }

        return builder.toMessage();
    }

    private static void writeQueryResponse(DataSerialization serialization, QueryResponse message) {
        writeClientResponse(serialization, message);

        if (message.getResults() != null) {
            serialization.writeBoolean(true);
            serialization.writeInt(message.getResults().size());
            for (QueryResponseItem result : message.getResults())
                writeQueryResponseItem(serialization, result);
        } else {
            serialization.writeBoolean(false);
        }
    }

    private static Message readQueryResponse(DataDeserialization deserialization) {
        QueryResponseBuilder builder = new QueryResponseBuilder();
        readClientResponse(deserialization, builder);

        if (deserialization.readBoolean()) {
            int count = deserialization.readInt();
            List<QueryResponseItem> results = new ArrayList<>(count);
            for (int i = 0; i < count; i++)
                results.add(readQueryResponseItem(deserialization));

            builder.setResults(results);
        }

        return builder.toMessage();
    }

    private static void writeQueryRequestItem(DataSerialization serialization, QueryRequestItem query) {
        serialization.writeLong(query.getLastPublishedMessageId());
        serialization.writeLong(query.getMessageId());
        serialization.writeByteArray(query.getValue());
    }

    private static QueryRequestItem readQueryRequestItem(DataDeserialization deserialization) {
        long lastPublishedMessageId = deserialization.readLong();
        long messageId = deserialization.readLong();
        ByteArray value = deserialization.readByteArray();

        return new QueryRequestItem(lastPublishedMessageId, messageId, value);
    }

    private static void writeQueryResponseItem(DataSerialization serialization, QueryResponseItem result) {
        serialization.writeLong(result.getMessageId());
        serialization.writeByteArray(result.getValue());
    }

    private static QueryResponseItem readQueryResponseItem(DataDeserialization deserialization) {
        long messageId = deserialization.readLong();
        ByteArray value = deserialization.readByteArray();

        return new QueryResponseItem(messageId, value);
    }

    private static void writeSubscriptionRequest(DataSerialization serialization, SubscriptionRequest message) {
        writeClientRequest(serialization, message);

        if (message.getSubscriptions() != null) {
            serialization.writeBoolean(true);
            serialization.writeInt(message.getSubscriptions().size());
            for (SubscriptionRequestItem subscription : message.getSubscriptions())
                writeSubscriptionRequestItem(serialization, subscription);
        } else {
            serialization.writeBoolean(false);
        }
    }

    private static Message readSubscriptionRequest(DataDeserialization deserialization) {
        SubscriptionRequestBuilder builder = new SubscriptionRequestBuilder();

        readClientRequest(deserialization, builder);

        if (deserialization.readBoolean()) {
            int count = deserialization.readInt();

            List<SubscriptionRequestItem> subscriptions = new ArrayList<>(count);
            for (int i = 0; i < count; i++)
                subscriptions.add(readSubscriptionRequestItem(deserialization));

            builder.setSubscriptions(subscriptions);
        }

        return builder.toMessage();
    }

    private static void writeSubscriptionResponse(DataSerialization serialization, SubscriptionResponse message) {
        writeClientResponse(serialization, message);

        if (message.getResults() != null) {
            serialization.writeBoolean(true);
            serialization.writeInt(message.getResults().size());
            for (SubscriptionResponseItem result : message.getResults())
                writeSubscriptionResponseItem(serialization, result);
        } else {
            serialization.writeBoolean(false);
        }
    }

    private static Message readSubscriptionResponse(DataDeserialization deserialization) {
        SubscriptionResponseBuilder builder = new SubscriptionResponseBuilder();
        readClientResponse(deserialization, builder);

        if (deserialization.readBoolean()) {
            int count = deserialization.readInt();
            List<SubscriptionResponseItem> results = new ArrayList<>(count);
            for (int i = 0; i < count; i++)
                results.add(readSubscriptionResponseItem(deserialization));

            builder.setResults(results);
        }

        return builder.toMessage();
    }

    private static void writeSubscriptionRequestItem(DataSerialization serialization, SubscriptionRequestItem subscription) {
        Serializers.writeUUID(serialization, subscription.getSubscriptionId());
        serialization.writeByteArray(subscription.getValue());
    }

    private static SubscriptionRequestItem readSubscriptionRequestItem(DataDeserialization deserialization) {
        UUID subscriptionId = Serializers.readUUID(deserialization);
        ByteArray value = deserialization.readByteArray();

        return new SubscriptionRequestItem(subscriptionId, value);
    }

    private static void writeSubscriptionResponseItem(DataSerialization serialization, SubscriptionResponseItem result) {
        Serializers.writeUUID(serialization, result.getSubscriptionId());
        serialization.writeByteArray(result.getValue());
    }

    private static SubscriptionResponseItem readSubscriptionResponseItem(DataDeserialization deserialization) {
        UUID subscriptionId = Serializers.readUUID(deserialization);
        ByteArray value = deserialization.readByteArray();

        return new SubscriptionResponseItem(subscriptionId, value);
    }

    private static void writeFollowerRequest(DataSerialization serialization, FollowerRequest message) {
        writeServerRequest(serialization, message);
    }

    private static void readFollowerRequest(DataDeserialization deserialization, FollowerRequestBuilder builder) {
        readServerRequest(deserialization, builder);
    }

    private static void writeFollowerResponse(DataSerialization serialization, FollowerResponse message) {
        writeServerResponse(serialization, message);

        if (message.getLeaderId() != null) {
            serialization.writeBoolean(true);
            Serializers.writeUUID(serialization, message.getLeaderId());
        } else {
            serialization.writeBoolean(false);
        }
    }

    private static void readFollowerResponse(DataDeserialization deserialization, FollowerResponseBuilder builder) {
        readServerResponse(deserialization, builder);

        if (deserialization.readBoolean())
            builder.setLeaderId(Serializers.readUUID(deserialization));
    }

    private static void writeClientRequest(DataSerialization serialization, ClientRequest message) {
        writeRequest(serialization, message);
    }

    private static void readClientRequest(DataDeserialization deserialization, ClientRequestBuilder builder) {
        readRequest(deserialization, builder);
    }

    private static void writeClientResponse(DataSerialization serialization, ClientResponse message) {
        writeResponse(serialization, message);

        serialization.writeBoolean(message.isAccepted());
    }

    private static void readClientResponse(DataDeserialization deserialization, ClientResponseBuilder builder) {
        readResponse(deserialization, builder);

        builder.setAccepted(deserialization.readBoolean());
    }

    private static void writeServerRequest(DataSerialization serialization, ServerRequest message) {
        writeRequest(serialization, message);
    }

    private static void readServerRequest(DataDeserialization deserialization, ServerRequestBuilder builder) {
        readRequest(deserialization, builder);
    }

    private static void writeServerResponse(DataSerialization serialization, ServerResponse message) {
        writeResponse(serialization, message);

        serialization.writeBoolean(message.isAccepted());
    }

    private static void readServerResponse(DataDeserialization deserialization, ServerResponseBuilder builder) {
        readResponse(deserialization, builder);

        builder.setAccepted(deserialization.readBoolean());
    }

    private static void writeRequest(DataSerialization serialization, Request message) {
        writeMessage(serialization, message);
    }

    private static void readRequest(DataDeserialization deserialization, RequestBuilder builder) {
        readMessage(deserialization, builder);
    }

    private static void writeResponse(DataSerialization serialization, Response message) {
        writeMessage(serialization, message);
    }

    private static void readResponse(DataDeserialization deserialization, ResponseBuilder builder) {
        readMessage(deserialization, builder);
    }

    private static void writeMessage(DataSerialization serialization, Message message) {
        Serializers.writeUUID(serialization, message.getGroupId());
        Serializers.writeUUID(serialization, message.getSource());
        Serializers.writeEnumSet(serialization, message.getFlags());
    }

    private static void readMessage(DataDeserialization deserialization, MessageBuilder builder) {
        builder.setGroupId(Serializers.readUUID(deserialization));
        builder.setSource(Serializers.readUUID(deserialization));
        builder.setFlags(Serializers.readEnumSet(deserialization, MessageFlags.class));
    }

    private MessageUtils() {
    }
}
