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

    public void setBatchWriteTimeWindowCheckInternal(int batchWriteTimeWindowCheckInternal) {
        this.batchWriteTimeWindowCheckInternal = batchWriteTimeWindowCheckInternal;
    }

    public int getBatchWriteTimeWindowCheckInternal() {
        return batchWriteTimeWindowCheckInternal;
    }

    private int batchWriteTimeWindowCheckInternal = 100;

    private int batchWriteWaitTimeThreshold = 5;

    private int batchWriteThreadSizeThreshold = 30;

    private int oneWriteMaxDataSize = 17 * 1024;

    private int batchWriteMemBufferSizeThreshold = (64 * 4 - 17) * 1024;

    private int batchWriteCommitLogMaxDataSize = batchWriteMemBufferSizeThreshold + oneWriteMaxDataSize;

    private void updateBatchWriteCommitLogMaxDataSize() {
        this. batchWriteCommitLogMaxDataSize = batchWriteMemBufferSizeThreshold + oneWriteMaxDataSize;
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

    //----------------------------------------------------

    private static final Config instance = new Config();

    public static Config getInstance() {
        return instance;
    }
}
