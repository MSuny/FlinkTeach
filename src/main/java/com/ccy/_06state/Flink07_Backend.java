package com.ccy._06state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_Backend {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new HashMapStateBackend());
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //Checkpoint过程中出现错误，是否让整体任务都失败，默认值为0，表示不容忍任何Checkpoint失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        //Checkpoint是进⾏失败恢复，当⼀个 Flink 应⽤程序失败终⽌、⼈为取消等时，它的 Checkpoint 就会被清除
        //可以配置不同策略进⾏操作
        // DELETE_ON_CANCELLATION: 当作业取消时，Checkpoint 状态信息会被删除，因此取消任务后，不能从Checkpoint 位置进⾏恢复任务
        // RETAIN_ON_CANCELLATION(多): 当作业⼿动取消时，将会保留作业的 Checkpoint 状态信息,要⼿动清除该作业的Checkpoint 状态信息
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //Flink 默认提供 Extractly-Once 保证 State 的⼀致性，还提供了 Extractly-Once，At-Least-Once 两种模式，
        // 设置checkpoint的模式为EXACTLY_ONCE，也是默认的，
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint的超时时间, 如果规定时间没完成则放弃，默认是10分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置同⼀时刻有多少个checkpoint可以同时执⾏，默认为1就⾏，以避免占⽤太多正常数据处理资源
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //设置了重启策略, 作业在失败后能⾃动恢复,失败后最多重启3次，每次重启间隔10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        env.enableCheckpointing(1000);
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
