import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class MysqlSource {
    public static void main(String[] args) throws Exception {
        JDBCInputFormat input = new JDBCInputFormat.JDBCInputFormatBuilder()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setUsername("root")
                .setPassword("123456")
                .setDBUrl("jdbc:mysql://localhost:3306/test_maxwell?serverTimezone=GMT%2b8")
                .setQuery("select * from test_flink")
                //设置获取的数据的类型
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
                .finish();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> input1 = env.createInput(input);
        input1.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                return row.toString();
            }
        }).print();

        env.execute();
        //离线批处理的print(),count(),collect()等都具有execute()的功能。即如果使用了这些就不需要提交execute()了
        //如果是流处理则必须提交execute()
    }
}
