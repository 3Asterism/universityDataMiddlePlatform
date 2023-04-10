package com.akisan.universityDataMiddlePlatform.service.impl;

import com.akisan.universityDataMiddlePlatform.entity.std_scorebar;
import com.akisan.universityDataMiddlePlatform.pojo.std_examCount;
import com.akisan.universityDataMiddlePlatform.service.reduceBarStream;
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
public class reduceBarStreamImpl implements reduceBarStream {
    @Override
    public void reduceBarStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行数为4
        env.setParallelism(4);
        readAndSinkMysql readMysql = new readAndSinkMysql();
        //获取数据源
        DataStream<Row> input1 = env.createInput(readMysql.testInput());

        //transformation
        DataStream<std_examCount> test_flinkDataStream = input1.map(new MapFunction<Row, std_examCount>() {
            @Override
            public std_examCount map(Row row) throws Exception {
                return new std_examCount(
                        (Integer) row.getField(0),
                        (Integer) row.getField(1),
                        (String) row.getField(2),
                        (String) row.getField(3),
                        (Integer) row.getField(4),
                        (String) row.getField(5),
                        (Integer) 1
                );
            }
        });

        //window assigner && window function
        //求分数总和
        SingleOutputStreamOperator<std_examCount> reduce = test_flinkDataStream.keyBy(std_examCount::getAcademy).window(EventTimeSessionWindows.withGap(Time.seconds(2))).reduce(new ReduceFunction<std_examCount>() {
            @Override
            public std_examCount reduce(std_examCount std_examCount, std_examCount t1) throws Exception {
                std_examCount.setScore(std_examCount.getScore() + t1.getScore());
                //计算该学院考试总次数
                std_examCount.setCount(std_examCount.getCount() + t1.getCount());
                return std_examCount;
            }
        });


        //归约为可下沉类型
        DataStream<std_scorebar> result = reduce.map(new MapFunction<std_examCount, std_scorebar>() {
            @Override
            public std_scorebar map(std_examCount stdExamCount) throws Exception {
                int avgScore = stdExamCount.getScore()/stdExamCount.getCount();
                return new std_scorebar(
                        (Integer) null,
                        (Integer) avgScore,
                        (String) stdExamCount.getAcademy()
                );
            }
        });

        //Ready to sink
        DataStream<Row> resultSink = result.map(new MapFunction<std_scorebar, Row>() {
            @Override
            public Row map(std_scorebar std_scorebar) throws Exception {
                Row row = new Row(3);
                row.setField(0, null);
                row.setField(1, std_scorebar.getAvgscore());
                row.setField(2, std_scorebar.getAcademy());
                return row;
            }
        });

        readAndSinkMysql sinkMysql = new readAndSinkMysql();

        String query = "INSERT INTO testdb.std_scorebar (id, avgscore, academy) VALUES (?, ?, ?)";

        //Sink
        resultSink.writeUsingOutputFormat(sinkMysql.testOutput(query));
        env.execute();
    }
}
