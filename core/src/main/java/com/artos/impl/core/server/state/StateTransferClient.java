package com.artos.impl.core.server.state;

import com.exametrika.common.config.resource.ClassPathResourceLoader;
import com.exametrika.common.config.resource.FileResourceLoader;
import com.exametrika.common.config.resource.IResourceLoader;
import com.exametrika.common.config.resource.ResourceManager;
import com.exametrika.common.l10n.DefaultMessage;
import com.exametrika.common.l10n.ILocalizedMessage;
import com.exametrika.common.l10n.Messages;
import com.exametrika.common.l10n.SystemException;
import com.exametrika.common.log.ILogger;
import com.exametrika.common.log.IMarker;
import com.exametrika.common.log.LogLevel;
import com.exametrika.common.log.Loggers;
import com.exametrika.common.messaging.ChannelException;
import com.exametrika.common.utils.Assert;
import com.exametrika.common.utils.Exceptions;
import com.exametrika.common.utils.Files;
import com.exametrika.common.utils.IOs;
import com.exametrika.common.utils.Times;

import javax.net.SocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

public class StateTransferClient {
    private  static int DEFAULT_BUFFER_SIZE = 8192;
    private static final IMessages messages = Messages.get(IMessages.class);
    private final ILogger logger = Loggers.get(getClass());
    private final IMarker marker;
    private final String host;
    private final int port;
    private final File workDir;
    private final boolean secured;
    private final String keyStoreFile;
    private final String keyStorePassword;
    private final IStateTransferApplier stateApplier;
    private volatile Socket socket;

    public StateTransferClient(String endpoint, String host, int port, String workDir,
                               boolean secured, String keyStoreFile, String keyStorePassword, IStateTransferApplier stateApplier) {
        Assert.notNull(host);
        Assert.notNull(workDir);
        Assert.notNull(stateApplier);
        if (secured) {
            Assert.notNull(keyStoreFile);
            Assert.notNull(keyStorePassword);
        }

        this.workDir = new File(workDir);
        if (!this.workDir.exists())
            Assert.isTrue(this.workDir.mkdirs());

        this.host = host;
        this.port = port;

        this.secured = secured;
        this.keyStoreFile = keyStoreFile;
        this.keyStorePassword = keyStorePassword;
        this.stateApplier = stateApplier;
        this.marker = Loggers.getMarker(host + ":" + port, Loggers.getMarker(endpoint));
    }

    public void transfer(UUID groupId, long logIndex) {
        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.requestSent(groupId, logIndex));

        Files.emptyDir(workDir);

        try (Socket socket = createSocket()) {
            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.connected(host, port, secured, workDir));

            this.socket = socket;
            writeRequest(groupId, logIndex, socket);
            readResponse(socket);
        } catch (Exception e) {
            Exceptions.wrapAndThrow(e);
        }

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.requestCompleted(groupId, logIndex));
    }

    public void close() {
        IOs.close(socket);
    }

    private void writeRequest(UUID groupId, long logIndex, Socket socket) throws IOException {
        BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());

        ByteBuffer buffer = ByteBuffer.allocate(24);
        buffer.putLong(groupId.getMostSignificantBits());
        buffer.putLong(groupId.getLeastSignificantBits());
        buffer.putLong(logIndex);
        buffer.flip();

        out.write(buffer.array(), buffer.arrayOffset(), buffer.limit());
        out.flush();
    }

    private void readResponse(Socket socket) throws IOException {
        BufferedInputStream in = new BufferedInputStream(socket.getInputStream());

        ByteBuffer buffer = ByteBuffer.allocate(4);
        in.read(buffer.array(), buffer.arrayOffset(), buffer.limit());

        int fileCount = buffer.getInt();

        if (logger.isLogEnabled(LogLevel.DEBUG))
            logger.log(LogLevel.DEBUG, marker, messages.filesInResponse(fileCount));

        for (int i = 0; i < fileCount; i++)
            transferFile(i, in);
    }

    private void transferFile(int fileIndex, InputStream in) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(18);
        in.read(buffer.array(), buffer.arrayOffset(), buffer.limit());

        boolean isSnapshot = buffer.get() == 1;
        boolean compressed = buffer.get() == 1;
        long logIndex = buffer.getLong();
        long fileSize = buffer.getLong();

        File file = File.createTempFile("raft" + fileIndex + "-", "tmp", workDir);

        try {
            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.startTransferFile(file, fileSize, isSnapshot));
            long start = Times.getCurrentTime();

            CRC32 crc = new CRC32();
            try (OutputStream out = new CheckedOutputStream(new BufferedOutputStream(new FileOutputStream(file)), crc)) {
                long checkSum = transfer(in, out, fileSize);
                if (crc.getValue() != checkSum)
                    throw new SystemException(messages.wrongCheckSum());
            }

            long delta = Times.getCurrentTime() - start;
            double rate = (double) fileSize / 1000000 * 1000 / delta;
            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.endTransferFile(file, fileSize, isSnapshot, delta, rate));

            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.startApplyState(file, fileSize, isSnapshot));
            start = Times.getCurrentTime();

            if (isSnapshot)
                stateApplier.applySnapshot(file, logIndex, compressed);
            else
                stateApplier.applyLog(file, compressed);

            delta = Times.getCurrentTime() - start;
            if (logger.isLogEnabled(LogLevel.DEBUG))
                logger.log(LogLevel.DEBUG, marker, messages.endApplyState(file, fileSize, isSnapshot, delta));
        } finally {
            file.delete();
        }
    }

    private long transfer(InputStream in, OutputStream out, long fileSize) throws IOException {
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        while (fileSize > 0) {
            long size = Math.min(DEFAULT_BUFFER_SIZE, fileSize);
            int read = in.read(buffer, 0, (int) size);
            if (read == -1)
                throw new SystemException(messages.endOfStream());

            out.write(buffer, 0, read);
            fileSize -= read;
        }

        in.read(buffer, 0, 8);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        long checkSum = byteBuffer.getLong();
        return checkSum;
    }

    private Socket createSocket() {
        try {
            SocketFactory socketFactory = createSocketFactory();
            return socketFactory.createSocket(host, port);
        } catch (Exception e) {
            return Exceptions.wrapAndThrow(e);
        }
    }

    private SocketFactory createSocketFactory() {
        if (!secured)
            return SocketFactory.getDefault();
        else
            return createSslContext().getSocketFactory();
    }

    private SSLContext createSslContext() {
        Map<String, IResourceLoader> resourceLoaders = new HashMap<String, IResourceLoader>();
        resourceLoaders.put(FileResourceLoader.SCHEMA, new FileResourceLoader());
        resourceLoaders.put(ClassPathResourceLoader.SCHEMA, new ClassPathResourceLoader());
        ResourceManager resourceManager = new ResourceManager(resourceLoaders, "file");
        InputStream stream = null;
        try {
            stream = resourceManager.getResource(keyStoreFile);
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(stream, keyStorePassword.toCharArray());
            SSLContext sslContext = SSLContext.getInstance("TLS");
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);

            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());

            return sslContext;
        } catch (Exception e) {
            throw new ChannelException(e);
        } finally {
            IOs.close(stream);
        }
    }

    private interface IMessages {
        @DefaultMessage("State transfer request has been sent to server. Group-id: {0}, log-index: {1}.")
        ILocalizedMessage requestSent(UUID groupId, long logIndex);

        @DefaultMessage("State transfer request has been completed. Group-id: {0}, log-index: {1}.")
        ILocalizedMessage requestCompleted(UUID groupId, long logIndex);

        @DefaultMessage("Received files in response: {0}.")
        ILocalizedMessage filesInResponse(int fileCount);

        @DefaultMessage("Start transferring file ''{0}'', file-size: {1}, snapshot: {2}.")
        ILocalizedMessage startTransferFile(File file, long fileSize, boolean isSnapshot);

        @DefaultMessage("End transferring file ''{0}'', file-size: {1}, snapshot: {2}, time(ms): {3}, rate(mb/s): {4}.")
        ILocalizedMessage endTransferFile(File file, long fileSize, boolean isSnapshot, long delta, double rate);

        @DefaultMessage("Start applying state ''{0}'', file-size: {1}, snapshot: {2}.")
        ILocalizedMessage startApplyState(File file, long fileSize, boolean isSnapshot);

        @DefaultMessage("End applying state ''{0}'', file-size: {1}, snapshot: {2}, time(ms): {3}.")
        ILocalizedMessage endApplyState(File file, long fileSize, boolean isSnapshot, long delta);

        @DefaultMessage("Wrong checksum.")
        ILocalizedMessage wrongCheckSum();

        @DefaultMessage("End of stream.")
        ILocalizedMessage endOfStream();

        @DefaultMessage("Client has been connected to server. Host: {0}, port: {1}, secured: {2}, work-dir: {3}")
        ILocalizedMessage connected(String host, int port, boolean secured, File workDir);
    }
}
