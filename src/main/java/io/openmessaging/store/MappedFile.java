package io.openmessaging.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author chenxi20
 * @date 2021/10/17
 */
public class MappedFile {

    private File file;

    private MappedByteBuffer mappedByteBuffer;

    private FileChannel fileChannel;

    private long fileSize = 1024 * 1024 * 1024;

    public MappedFile(String fileName) throws IOException {
        this.file = new File(fileName);
        this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
    }

    public void appendMessage() {

    }
}
