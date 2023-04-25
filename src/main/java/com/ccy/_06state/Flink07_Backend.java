package com.ccy._06state;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_Backend {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 启用检查点，间隔时间1秒
        env.enableCheckpointing(1000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 设置精确一次模式
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 最小间隔时间500毫秒
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 超时时间1分钟
        checkpointConfig.setCheckpointTimeout(60000);
        // 同时只能有一个检查点
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 开启检查点的外部持久化保存，作业取消后依然保留
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 启用不对齐的检查点保存方式
        checkpointConfig.enableUnalignedCheckpoints();
        // 设置检查点存储，可以直接传入一个String，指定文件系统的路径
        checkpointConfig.setCheckpointStorage("hdfs://my/checkpoint/dir");
    }
}
