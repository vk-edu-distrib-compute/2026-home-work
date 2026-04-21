package company.vk.edu.distrib.compute.mediocritas.storage;

import company.vk.edu.distrib.compute.Dao;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static company.vk.edu.distrib.compute.mediocritas.storage.validation.DaoKeyValidatorUtils.validateKey;

public class FileByteDao implements Dao<byte[]> {

    private static final String BASE_PATH = "./data";
    private static final String FILE_NAME = "data.bin";

    private static final int TOMBSTONE_SIZE = 1;
    private static final int KEY_LENGTH_SIZE = 4;
    private static final int VALUE_LENGTH_SIZE = 4;
    private static final int HEADER_SIZE = TOMBSTONE_SIZE + KEY_LENGTH_SIZE + VALUE_LENGTH_SIZE;

    private static final byte ALIVE = 0;
    private static final byte DELETED = 1;

    private final Map<String, Long> index;
    private final AtomicLong writePosition;
    private final FileChannel channel;

    public FileByteDao() throws IOException {
        this(BASE_PATH);
    }

    public FileByteDao(String basePath) throws IOException {
        this.index = new ConcurrentHashMap<>();
        Path dir = Path.of(basePath);
        Files.createDirectories(dir);
        Path dataFile = dir.resolve(FILE_NAME);
        this.channel = FileChannel.open(dataFile,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
        this.writePosition = new AtomicLong(channel.size());
        restoreIndex();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IOException {
        Long offset = index.get(key);
        if (offset == null) {
            throw new NoSuchElementException("No element with id: " + key);
        }

        ByteBuffer tombstoneBuffer = ByteBuffer.allocate(TOMBSTONE_SIZE);
        readFully(tombstoneBuffer, offset);
        tombstoneBuffer.flip();

        if (tombstoneBuffer.get() == DELETED) {
            throw new NoSuchElementException("No element with id: " + key);
        }

        ByteBuffer valueLenBuffer = ByteBuffer.allocate(VALUE_LENGTH_SIZE);
        readFully(valueLenBuffer, offset + TOMBSTONE_SIZE + KEY_LENGTH_SIZE);
        valueLenBuffer.flip();
        int length = valueLenBuffer.getInt();

        ByteBuffer dataBuffer = ByteBuffer.allocate(length);
        readFully(dataBuffer, offset + HEADER_SIZE);
        dataBuffer.flip();
        return dataBuffer.array();
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);

        int recordSize = HEADER_SIZE + value.length + key.length();
        long offset = writePosition.getAndAdd(recordSize);

        ByteBuffer buf = ByteBuffer.allocate(recordSize)
                .put(ALIVE)
                .putInt(key.length())
                .putInt(value.length)
                .put(value)
                .put(key.getBytes());

        buf.flip();

        writeFully(buf, offset);
        index.put(key, offset);
    }

    @Override
    public void delete(String key) throws IOException {
        Long offset = index.get(key);
        if (offset == null) {
            return;
        }

        ByteBuffer buf = ByteBuffer.allocate(TOMBSTONE_SIZE).put(DELETED);
        buf.flip();
        writeFully(buf, offset);
        index.remove(key);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private void readFully(ByteBuffer buf, long offset) throws IOException {
        int total = buf.remaining();
        while (buf.hasRemaining()) {
            int bytesRead = channel.read(buf, offset + (total - buf.remaining()));
            if (bytesRead == -1) {
                throw new EOFException("Unexpected end of file at offset " + offset);
            }
        }
    }

    private void writeFully(ByteBuffer buf, long offset) throws IOException {
        int total = buf.remaining();
        while (buf.hasRemaining()) {
            channel.write(buf, offset + (total - buf.remaining()));
        }
    }

    private void restoreIndex() throws IOException {
        long fileSize = channel.size();
        long offset = 0;

        while (offset < fileSize) {
            ByteBuffer tombstoneBuffer = ByteBuffer.allocate(TOMBSTONE_SIZE);
            readFully(tombstoneBuffer, offset);
            tombstoneBuffer.flip();

            ByteBuffer keyLenBuffer = ByteBuffer.allocate(KEY_LENGTH_SIZE);
            readFully(keyLenBuffer, offset + TOMBSTONE_SIZE);
            keyLenBuffer.flip();
            int keyLength = keyLenBuffer.getInt();

            ByteBuffer valueLenBuffer = ByteBuffer.allocate(VALUE_LENGTH_SIZE);
            readFully(valueLenBuffer, offset + TOMBSTONE_SIZE + KEY_LENGTH_SIZE);
            valueLenBuffer.flip();
            int valueLength = valueLenBuffer.getInt();

            ByteBuffer keyBuffer = ByteBuffer.allocate(keyLength);
            readFully(keyBuffer, offset + HEADER_SIZE + valueLength);
            keyBuffer.flip();
            String key = StandardCharsets.UTF_8.decode(keyBuffer).toString();

            byte tombstone = tombstoneBuffer.get();
            if (tombstone == ALIVE) {
                index.put(key, offset);
            } else if (tombstone == DELETED) {
                index.remove(key);
            } else {
                throw new IOException("Corrupted file: unknown tombstone value " + tombstone + " at offset " + offset);
            }

            offset += HEADER_SIZE + valueLength + keyLength;
        }
    }
}
