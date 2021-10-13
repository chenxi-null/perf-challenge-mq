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

    private static final Config instance = new Config();

    public static Config getInstance() {
        return instance;
    }
}
