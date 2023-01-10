package com.akisan.universityDataMiddlePlatform.util;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class readAndSinkMysql {
    public JDBCInputFormat testInput(){
        JDBCInputFormat input = new JDBCInputFormat.JDBCInputFormatBuilder()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setUsername("root")
                .setPassword("123456")
                .setDBUrl("jdbc:mysql://localhost:3306/test_maxwell?serverTimezone=GMT%2b8")
                .setQuery("select * from test_flink")
                //设置获取的数据的类型
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO))
                .finish();
        return input;
    }

    public OutputFormat<Row> testOutput(String query){
        JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setUsername("root")
                .setPassword("123456")
                .setDBUrl("jdbc:mysql://localhost:3306/test_maxwell?serverTimezone=GMT%2b8")
                .setQuery(query)
                .finish();
        return jdbcOutput;
    }
}
