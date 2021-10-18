package io.openmessaging.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chenxi20
 * @date 2021/10/17
 */
public class MappedFile {

    private static final Logger log = LoggerFactory.getLogger(MappedFile.class);

    private final File file;

    private final MappedByteBuffer mappedByteBuffer;

    private final FileChannel fileChannel;

    private final long fileSize = 1024 * 1024 * 1024;

    private final AtomicInteger wrotePosition = new AtomicInteger();

    public MappedFile(String fileName) throws IOException {
        this.file = new File(fileName);
        this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
    }

    public boolean appendMessage(ByteBuffer data) {
        int length = data.remaining();

        int currPos = wrotePosition.get();
        if ((currPos + length) <= this.fileSize) {
            try {
                fileChannel.position(currPos);
                fileChannel.write(data);
            } catch (Throwable e) {
                log.error("error occurred when append message to mappedFile", e);
            }
            wrotePosition.addAndGet(length);
            return true;
        }
        return false;
    }
}
