package io.openmessaging.store;

import io.openmessaging.Config;
import io.openmessaging.util.FileUtil;
import io.openmessaging.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * @author chenxi20
 * @date 2021/10/10
 */
public class Checkpoint {

    private static final Logger log = LoggerFactory.getLogger(Checkpoint.class);

    private final FileChannel writeFileChannel;

    private final FileChannel readFileChannel;

    private final ByteBuffer byteBuffer;

    public Checkpoint() throws IOException {
        Path checkpointPath = Config.getInstance().getCheckpointPath();

        FileUtil.createFileIfNotExists(checkpointPath);

        this.writeFileChannel = FileChannel.open(checkpointPath,
                StandardOpenOption.WRITE, StandardOpenOption.APPEND);

        this.readFileChannel = FileChannel.open(checkpointPath,
                StandardOpenOption.READ);

        this.byteBuffer = ByteBuffer.allocate(8);

        // init
        updatePhyOffset(0L);
    }

    public long getPhyOffset() throws IOException {
        byteBuffer.clear();
        int readBytes = readFileChannel.read(byteBuffer, 0);
        Util.assertTrue(readBytes == 8);
        byteBuffer.flip();
        return byteBuffer.getLong();
    }

    public void updatePhyOffset(long nextPhyOffset) throws IOException {
        log.info("checkpoint, updatePhyOffset: " + nextPhyOffset);
        byteBuffer.clear();
        byteBuffer.putLong(nextPhyOffset);
        byteBuffer.flip();
        writeFileChannel.write(byteBuffer);
        writeFileChannel.force(true);
    }
}
