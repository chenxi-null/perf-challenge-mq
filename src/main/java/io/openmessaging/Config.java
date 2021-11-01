package io.openmessaging;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author chenxi20
 * @date 2021/10/8
 */
public class Config {

    private static final String DEFAULT_ROOT_DIR = "/essd/mqx";

    private boolean enableConsumeQueueDataSync = true;

    private Path rootDirPath;

    private Path commitLogPath;

    private String consumerQueueRootDir;

    private Path checkpointPath;


    private Config() {
        updateBatchWriteCommitLogMaxDataSize();
        setRootDir(DEFAULT_ROOT_DIR);
    }

    public void setEnableConsumeQueueDataSync(boolean enableConsumeQueueDataSync) {
        this.enableConsumeQueueDataSync = enableConsumeQueueDataSync;
    }

    public void setRootDir(String rootDir) {
        this.rootDirPath = Paths.get(rootDir);

        String commitLogFile = rootDir + "/commitlog";;
        this.commitLogPath = Paths.get(commitLogFile);

        this.consumerQueueRootDir = rootDir;

        String checkpointFile = rootDir + "/checkpoint";
        this.checkpointPath = Paths.get(checkpointFile);
    }

    public Path getRootDirPath() {
        return rootDirPath;
    }

    public Path getCheckpointPath() {
        return checkpointPath;
    }

    public Path getCommitLogPath() {
        return commitLogPath;
    }

    public String getConsumerQueueRootDir() {
        return consumerQueueRootDir;
    }

    public boolean isEnableConsumeQueueDataSync() {
        return enableConsumeQueueDataSync;
    }

    //----------------------------------------------------

    private final int topicMaxByteNum = 100;

    // 4 /* logSize */
    // 4 /* msgSize */
    // msgSize /* data */
    // 4 /* queueId */
    // 8 /* queueOffset */
    // topicBytes.length
    // 4 /* checksum */;
    public int getLogItemMaxByteNum() {
        return 4 + 4 + getOneWriteMaxDataSize() + 4 + 8 + getTopicMaxByteNum() + 4;
    }

    public int getTopicMaxByteNum() {
        return topicMaxByteNum;
    }

    //----------------------------------------------------

    private int batchWriteThreadSizeThreshold = 30;

    private int oneWriteMaxDataSize = 17 * 1024;

    private int batchWriteMemBufferSizeThreshold = (64 * 4 - 17) * 1024;

    private int batchWriteCommitLogMaxDataSize;

    private int batchWriteTimeWindowCheckInternal = 2;

    private int batchWriteWaitTimeThreshold = 4;

    private void updateBatchWriteCommitLogMaxDataSize() {
        this. batchWriteCommitLogMaxDataSize = batchWriteMemBufferSizeThreshold + 2 * oneWriteMaxDataSize;
    }

    public void setOneWriteMaxDataSize(int oneWriteMaxDataSize) {
        this.oneWriteMaxDataSize = oneWriteMaxDataSize;
        updateBatchWriteCommitLogMaxDataSize();
    }

    public void setBatchWriteMemBufferSizeThreshold(int batchWriteMemBufferSizeThreshold) {
        this.batchWriteMemBufferSizeThreshold = batchWriteMemBufferSizeThreshold;
        updateBatchWriteCommitLogMaxDataSize();
    }

    public int getOneWriteMaxDataSize() {
        return oneWriteMaxDataSize;
    }

    public int getBatchWriteCommitLogMaxDataSize() {
        return batchWriteCommitLogMaxDataSize;
    }

    public int getBatchWriteMemBufferSizeThreshold() {
        return batchWriteMemBufferSizeThreshold;
    }

    public int getBatchWriteWaitTimeThreshold() {
        return batchWriteWaitTimeThreshold;
    }

    public int getBatchWriteThreadSizeThreshold() {
        return batchWriteThreadSizeThreshold;
    }

    public void setBatchWriteWaitTimeThreshold(int batchWriteWaitTimeThreshold) {
        this.batchWriteWaitTimeThreshold = batchWriteWaitTimeThreshold;
    }

    public void setBatchWriteThreadSizeThreshold(int batchWriteThreadSizeThreshold) {
        this.batchWriteThreadSizeThreshold = batchWriteThreadSizeThreshold;
    }

    public void setBatchWriteTimeWindowCheckInternal(int batchWriteTimeWindowCheckInternal) {
        this.batchWriteTimeWindowCheckInternal = batchWriteTimeWindowCheckInternal;
    }

    public int getBatchWriteTimeWindowCheckInternal() {
        return batchWriteTimeWindowCheckInternal;
    }

    //----------------------------------------------------

    private static final Config instance = new Config();

    public static Config getInstance() {
        return instance;
    }

    //----------------------------------------------------

    // queueNum: 50w
    // max size of msg = 1,310,720 -> 130w
    //
    // max size of index = max size of msg * 16 -> 20M
    //  (125 * 1024 * 1024 * 1024) / (100 * 1024) * 16 -> 20,971,520 ( 20M)

    public long pmemMsgHeapSize = (60L - 2) * 1024 * 1024 * 1024;

    public long pmemIndexHeapSize = 20 * 1024 * 1024;

    public long pmemIndexMemoryBlockSize = 16 * 5;

    public String pmemDir = "/pmem";

    public String getPmemDir() {
        return pmemDir;
    }

    public void setPmemDir(String pmemDir) {
        this.pmemDir = pmemDir;
    }

    public String getPmemMsgHeapPath() {
        return getPmemDir() + "/msg_heap";
    }

    public long getPmemMsgHeapSize() {
        return pmemMsgHeapSize;
    }

    public void setPmemMsgHeapSize(long pmemMsgHeapSize) {
        this.pmemMsgHeapSize = pmemMsgHeapSize;
    }

    public long getPmemIndexHeapSize() {
        return pmemIndexHeapSize;
    }

    public void setPmemIndexHeapSize(long pmemIndexHeapSize) {
        this.pmemIndexHeapSize = pmemIndexHeapSize;
    }

    public long getPmemIndexMemoryBlockSize() {
        return pmemIndexMemoryBlockSize;
    }

    public void setPmemIndexMemoryBlockSize(long pmemIndexMemoryBlockSize) {
        this.pmemIndexMemoryBlockSize = pmemIndexMemoryBlockSize;
    }
}
