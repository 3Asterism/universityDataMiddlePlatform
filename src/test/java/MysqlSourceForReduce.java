import com.akisan.universityDataMiddlePlatform.entity.test_flink;
import com.akisan.universityDataMiddlePlatform.util.readAndSinkMysql;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class MysqlSourceForReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        readAndSinkMysql readMysql = new readAndSinkMysql();
        //获取数据源
        DataStream<Row> input1 = env.createInput(readMysql.testInput());

        //transformation
        DataStream<test_flink> test_flinkDataStream = input1.map(new MapFunction<Row, test_flink>() {
            @Override
            public test_flink map(Row row) throws Exception {
                return new test_flink(
                        (Integer) row.getField(0),
                        (String) row.getField(1),
                        (Integer) row.getField(2)
                );
            }
        });


        //Aggregation reduce
        SingleOutputStreamOperator<test_flink> reduce = test_flinkDataStream.keyBy(test_flink::getName).reduce(new ReduceFunction<test_flink>() {
            @Override
            public test_flink reduce(test_flink test_flink, test_flink t1) throws Exception {
                test_flink.setAge(test_flink.getAge() + t1.getAge());
                return test_flink;
            }
        });

        SingleOutputStreamOperator<test_flink> result = reduce.keyBy(test_flink -> "key").reduce(new ReduceFunction<test_flink>() {
            @Override
            public test_flink reduce(test_flink test_flink, test_flink t1) throws Exception {
                return test_flink.getAge() >= t1.getAge() ? test_flink : t1;
            }
        });

        //filter
        SingleOutputStreamOperator<test_flink> filter = result.filter(new FilterFunction<test_flink>() {
            @Override
            public boolean filter(test_flink test_flink) throws Exception {
                return test_flink.getAge() >= 8;
            }
        });

        //Ready to sink
        DataStream<Row> resultSink = filter.map(new MapFunction<test_flink, Row>() {
            @Override
            public Row map(test_flink test_flink) throws Exception {
                Row row = new Row(3);
                row.setField(0, test_flink.getId());
                row.setField(1, test_flink.getName());
                row.setField(2, test_flink.getAge());
                return row;
            }
        });

        readAndSinkMysql sinkMysql = new readAndSinkMysql();

        String query = "INSERT INTO test_maxwell.test_flinksink (id, name, age) VALUES (?, ?, ?)";

        //Sink
        resultSink.writeUsingOutputFormat(sinkMysql.testOutput(query));
        env.execute();
    }
}
