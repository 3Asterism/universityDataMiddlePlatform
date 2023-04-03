package com.akisan.universityDataMiddlePlatform.service.impl;

import com.akisan.universityDataMiddlePlatform.entity.std_actvalarm;
import com.akisan.universityDataMiddlePlatform.pojo.std_actvCount;
import com.akisan.universityDataMiddlePlatform.service.reduceActvStream;
import com.akisan.universityDataMiddlePlatform.util.readAndSinkMysql;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.springframework.stereotype.Service;

@Service
public class reduceActvStreamImpl implements reduceActvStream {

    @Override
    public void reduceActvStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行数为4
        env.setParallelism(4);
        readAndSinkMysql readMysql = new readAndSinkMysql();
        //获取数据源
        DataStream<Row> input1 = env.createInput(readMysql.actvAlarmInput());

        //transformation
        DataStream<std_actvCount> test_flinkDataStream = input1.map(new MapFunction<Row, std_actvCount>() {
            @Override
            public std_actvCount map(Row row) throws Exception {
                return new std_actvCount(
                        (Integer) row.getField(0),
                        (Integer) row.getField(1),
                        (String) row.getField(2),
                        (String) row.getField(3),
                        (Integer) row.getField(4),
                        (Integer) 1
                );
            }
        });

        //window assigner && window function
        //求分数总和
        SingleOutputStreamOperator<std_actvCount> reduce = test_flinkDataStream.keyBy(std_actvCount::getName).window(EventTimeSessionWindows.withGap(Time.seconds(2))).reduce(new ReduceFunction<std_actvCount>() {
            @Override
            public std_actvCount reduce(std_actvCount std_actvCount, std_actvCount t1) throws Exception {
                //计算该人活动出现次数
                std_actvCount.setCount(std_actvCount.getCount() + t1.getCount());
                return std_actvCount;
            }
        });


        //归约为可下沉类型
        DataStream<std_actvalarm> result = reduce.map(new MapFunction<std_actvCount, std_actvalarm>() {
            @Override
            public std_actvalarm map(std_actvCount std_actvCount) throws Exception {
                int avgScore = std_actvCount.getCount();
                int ifAttempt;
                //如果实际参加次数 大于 实际参加次数
                if(std_actvCount.getCount() > std_actvCount.getAttempt()){
                    ifAttempt = 1;
                }else{
                    ifAttempt = 0;
                }
                return new std_actvalarm(
                        (Integer) null,
                        (Integer) std_actvCount.getStdid(),
                        (String) std_actvCount.getName(),
                        (Integer) ifAttempt
                );
            }
        });

        //Ready to sink
        DataStream<Row> resultSink = result.map(new MapFunction<std_actvalarm, Row>() {
            @Override
            public Row map(std_actvalarm std_actvalarm) throws Exception {
                Row row = new Row(4);
                row.setField(0, std_actvalarm.getId());
                row.setField(1, std_actvalarm.getStdid());
                row.setField(2, std_actvalarm.getName());
                row.setField(3, std_actvalarm.getIfattempt());
                return row;
            }
        });

        readAndSinkMysql sinkMysql = new readAndSinkMysql();

        String query = "INSERT INTO testdb.std_actvalarm (id, stdid, name, ifattempt) VALUES (?, ?, ?, ?)";

        //Sink
        resultSink.writeUsingOutputFormat(sinkMysql.testOutput(query));
        env.execute();
    }
}
