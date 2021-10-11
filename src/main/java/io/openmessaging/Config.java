package io.openmessaging;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author chenxi20
 * @date 2021/10/8
 */
public class Config {

    private static final Config instance = new Config();

    private String commitLogFile = "/essd/commitlog";;

    private Path commitLogPath = Paths.get(commitLogFile);

    private String consumerQueueRootDir = "/essd";

    private boolean enableConsumeQueueDataSync = true;


    public boolean isEnableConsumeQueueDataSync() {
        return enableConsumeQueueDataSync;
    }

    public void setEnableConsumeQueueDataSync(boolean enableConsumeQueueDataSync) {
        this.enableConsumeQueueDataSync = enableConsumeQueueDataSync;
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

    //----------------------------------------------------

    public static Config getInstance() {
        return instance;
    }

    private Config() {}
}
