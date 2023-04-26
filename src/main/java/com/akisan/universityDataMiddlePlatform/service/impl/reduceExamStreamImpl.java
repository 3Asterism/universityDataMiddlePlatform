package com.akisan.universityDataMiddlePlatform.service.impl;

import com.akisan.universityDataMiddlePlatform.entity.std_info;
import com.akisan.universityDataMiddlePlatform.mapper.std_infoMapper;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

//处理std_exam中的数据 输出及格率
@Service
public class reduceExamStreamImpl implements reduceExamStream {
    @Autowired
    private std_infoMapper std_infoMapper;

    @Scheduled(cron = "0 13 17 * * *")
    @Override
    public void reduceExamStream() throws Exception {
        std_infoMapper.deleteStdScoreAlarm();
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
        SingleOutputStreamOperator<std_examCount> reduce = test_flinkDataStream.keyBy(std_examCount::getName).window(EventTimeSessionWindows.withGap(Time.seconds(2))).reduce(new ReduceFunction<std_examCount>() {
            @Override
            public std_examCount reduce(std_examCount std_examCount, std_examCount t1) throws Exception {
                std_examCount.setScore(std_examCount.getScore() + t1.getScore());
                //计算该人考试出现次数
                std_examCount.setCount(std_examCount.getCount() + t1.getCount());
                return std_examCount;
            }
        });


        //归约为可下沉类型
        DataStream<std_info> result = reduce.map(new MapFunction<std_examCount, std_info>() {
            @Override
            public std_info map(std_examCount stdExamCount) throws Exception {
                int avgScore = stdExamCount.getScore()/stdExamCount.getCount();
                String ifPass;
                if(avgScore >=60){
                    ifPass = "Pass";
                }else{
                    ifPass = "Fail";
                }
                return new std_info(
                        (Integer) stdExamCount.getStdid(),
                        (String) stdExamCount.getName(),
                        (String) null,
                        (String) null,
                        (String) ifPass,
                        (String) null,
                        (String) "考试平均分不达标"
                );
            }
        });

        //Ready to sink
        DataStream<Row> resultSink = result.map(new MapFunction<std_info, Row>() {
            @Override
            public Row map(std_info std_info) throws Exception {
                Row row = new Row(7);
                row.setField(0, std_info.getId());
                row.setField(1, std_info.getName());
                row.setField(2, std_info.getClassname());
                row.setField(3, std_info.getInschool());
                row.setField(4, std_info.getPassrate());
                row.setField(5, std_info.getActvrate());
                row.setField(6, std_info.getResult());
                return row;
            }
        });

        readAndSinkMysql sinkMysql = new readAndSinkMysql();

        String query = "INSERT INTO testdb.std_info (id, name, classname, inschool, passrate, actvrate, result) VALUES (?, ?, ?, ?, ?, ?, ?)";

        //Sink
        resultSink.writeUsingOutputFormat(sinkMysql.testOutput(query));
        env.execute();
    }
}
