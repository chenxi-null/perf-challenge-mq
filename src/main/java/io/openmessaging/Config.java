package io.openmessaging;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author chenxi20
 * @date 2021/10/8
 */
public class Config {

    private String commitLogFile = "/essd/commitlog";;

    private Path commitLogPath = Paths.get(commitLogFile);

    private String consumerQueueRootDir = "/essd";

    private boolean enableConsumeQueueDataSync = true;

    private String checkpointFile = "/essd/checkpoint";

    private Path checkpointPath = Paths.get(checkpointFile);


    public void setCheckpointFile(String checkpointFile) {
        this.checkpointFile = checkpointFile;
        this.checkpointPath = Paths.get(this.checkpointFile);
    }

    public Path getCheckpointPath() {
        return checkpointPath;
    }

    public String getCommitLogFile() {
        return commitLogFile;
    }

    public void setCommitLogFile(String commitLogFile) {
        this.commitLogFile = commitLogFile;
        this.commitLogPath = Paths.get(this.commitLogFile);
    }

    public Path getCommitLogPath() {
        return commitLogPath;
    }

    public String getConsumerQueueRootDir() {
        return consumerQueueRootDir;
    }

    public void setConsumerQueueRootDir(String consumerQueueRootDir) {
        this.consumerQueueRootDir = consumerQueueRootDir;
    }

    public boolean isEnableConsumeQueueDataSync() {
        return enableConsumeQueueDataSync;
    }

    public void setEnableConsumeQueueDataSync(boolean enableConsumeQueueDataSync) {
        this.enableConsumeQueueDataSync = enableConsumeQueueDataSync;
    }

    //----------------------------------------------------

    private static final Config instance = new Config();

    public static Config getInstance() {
        return instance;
    }

    private Config() {}
}
