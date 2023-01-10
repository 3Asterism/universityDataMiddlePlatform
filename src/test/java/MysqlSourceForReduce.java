import com.akisan.universityDataMiddlePlatform.entity.test_flink;
import com.akisan.universityDataMiddlePlatform.util.readMysql;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class MysqlSourceForReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        readMysql readMysql = new readMysql();
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

        //reduce
        test_flinkDataStream.keyBy(test_flink::getName).reduce(new ReduceFunction<test_flink>() {
            @Override
            public test_flink reduce(test_flink test_flink, test_flink t1) throws Exception {
                test_flink.setAge(test_flink.getAge()+t1.getAge());
                return test_flink;
            }
        }).print();

        env.execute();
    }
}
