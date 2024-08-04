package com.sfl.flink.ds04Sink;

import com.sfl.flink.bean.WaterSensor;
import com.sfl.flink.source.GenWaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZoneId;

public class Test02 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        //  2 ，源 ：
        DataGeneratorSource<WaterSensor> source = new DataGeneratorSource<>(new GenWaterSensor(), Long.MAX_VALUE, RateLimiterStrategy.perSecond(2), Types.POJO(WaterSensor.class));
        DataStreamSource<WaterSensor> dsSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "waterSensor");

        //  3 ，输出 ：
        //      3.1 ，sql ：
        String sql = "insert into ws values(?,?,?)";
        //      3.2 ，数据 ：
        JdbcStatementBuilder<WaterSensor> jdbcState = new JdbcStatementBuilder<WaterSensor>() {
            @Override
            public void accept(PreparedStatement ps, WaterSensor waterSensor) throws SQLException {
                ps.setInt(1, waterSensor.getId());
                ps.setLong(2, waterSensor.getTs());
                ps.setInt(3, waterSensor.getVc());
            }
        };
        //      3.3 ，jdbc 执行选项 ：
        JdbcExecutionOptions jdbcPool = JdbcExecutionOptions.builder()
                .withMaxRetries(3) // 重试次数
                .withBatchSize(100) // 批次的大小：条数
                .withBatchIntervalMs(3000) // 批次的时间
                .build();
        //      3.4 ，连接信息 ：
        JdbcConnectionOptions jdbcInfo = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://cls02:3306/sfl?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                .withUsername("root")
                .withPassword("123456")
                .withConnectionCheckTimeoutSeconds(60) // 重试的超时时间
                .build();

        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink(sql,jdbcState,jdbcPool,jdbcInfo);

        //  5 ，打印 ：
        dsSource.addSink(jdbcSink);



        //  6 ，执行 ：
        env.execute();
    }

}
