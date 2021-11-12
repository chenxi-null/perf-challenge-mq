package io.openmessaging.perf;

import lombok.SneakyThrows;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author chenxi20
 * @date 2021/11/11
 */
public class EvaluateRecorder {

    @SneakyThrows
    public void record(long costTime, long wroteSize, int wroteTotalNum) {
        File file = new File("eval/write-record.txt");
        Path path = file.toPath();
        String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String content = String.format("%s | %7d | %15d | %7d\n", time, costTime, wroteSize, wroteTotalNum);
        Files.write(path, content.getBytes(), StandardOpenOption.APPEND);
    }
}
