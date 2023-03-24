package com.akisan.universityDataMiddlePlatform.service.impl;

import com.akisan.universityDataMiddlePlatform.entity.std_info;
import com.akisan.universityDataMiddlePlatform.pojo.std_examCount;
import com.akisan.universityDataMiddlePlatform.service.reduceExamStream;
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

//处理std_exam中的数据 输出及格率
@Service
public class reduceExamStreamImpl implements reduceExamStream {

    @Override
    public void reduceExamStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行数为8
        env.setParallelism(8);
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
                        (Integer) row.getField(5)
                );
            }
        });

        //window assigner && window function
        //求分数总和
        SingleOutputStreamOperator<std_examCount> reduceAvg = test_flinkDataStream.keyBy(std_examCount::getName).window(EventTimeSessionWindows.withGap(Time.seconds(2))).reduce(new ReduceFunction<std_examCount>() {
            @Override
            public std_examCount reduce(std_examCount std_examCount, std_examCount t1) throws Exception {
                std_examCount.setScore(std_examCount.getScore() + t1.getScore());
                //计算该人考试出现次数
                std_examCount.setCount(std_examCount.getScore()+1);
                return std_examCount;
            }
        });

        //求平均值
        SingleOutputStreamOperator<std_examCount> reduce =  reduceAvg.keyBy(std_examCount::getName).window(EventTimeSessionWindows.withGap(Time.seconds(2))).reduce(new ReduceFunction<std_examCount>() {
            @Override
            public std_examCount reduce(std_examCount std_exam, std_examCount t1) throws Exception {
                //总数除出现次数得平均数
                std_exam.setScore((std_exam.getScore() + t1.getScore())/t1.getCount());
                return std_exam;
            }
        });

        //归约为可下沉类型
        DataStream<std_info> result = reduce.map(new MapFunction<std_examCount, std_info>() {
            @Override
            public std_info map(std_examCount stdExamCount) throws Exception {
                return new std_info(
                        (Integer) stdExamCount.getStdid(),
                        (String) stdExamCount.getName(),
                        (String) null,
                        (String) null,
                        (String) String.valueOf(stdExamCount.getScore()),
                        (String) null
                );
            }
        });

        //Ready to sink
        DataStream<Row> resultSink = result.map(new MapFunction<std_info, Row>() {
            @Override
            public Row map(std_info std_info) throws Exception {
                Row row = new Row(6);
                row.setField(0, std_info.getId());
                row.setField(1, std_info.getName());
                row.setField(2, std_info.getClassname());
                row.setField(3, std_info.getInschool());
                row.setField(4, std_info.getPassrate());
                row.setField(5, std_info.getActvrate());
                return row;
            }
        });

        readAndSinkMysql sinkMysql = new readAndSinkMysql();

        String query = "INSERT INTO test_maxwell.std_info (id, name, classname, inschool, passrate, actvrate) VALUES (?, ?, ?, ?, ?, ?)";

        //Sink
        resultSink.writeUsingOutputFormat(sinkMysql.testOutput(query));
        env.execute();
    }
}
