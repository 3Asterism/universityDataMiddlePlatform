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
                .setPassword("!Baicaiin33")
                .setDBUrl("jdbc:mysql://47.100.215.126:3306/testdb?characterEncoding=utf8")
                .setQuery("select * from std_exam")
                //设置获取的数据的类型
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
                .finish();
        return input;
    }

    public OutputFormat<Row> testOutput(String query){
        JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setUsername("root")
                .setPassword("!Baicaiin33")
                .setDBUrl("jdbc:mysql://47.100.215.126:3306/testdb?characterEncoding=utf8")
                .setQuery(query)
                .finish();
        return jdbcOutput;
    }

    public JDBCInputFormat actvAlarmInput(){
        JDBCInputFormat input = new JDBCInputFormat.JDBCInputFormatBuilder()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setUsername("root")
                .setPassword("!Baicaiin33")
                .setDBUrl("jdbc:mysql://47.100.215.126:3306/testdb?characterEncoding=utf8")
                .setQuery("select * from std_actv")
                //设置获取的数据的类型
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO))
                .finish();
        return input;
    }
}
