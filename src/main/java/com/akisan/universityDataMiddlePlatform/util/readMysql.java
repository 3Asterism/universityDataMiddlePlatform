package com.akisan.universityDataMiddlePlatform.util;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

public class readMysql {
    JDBCInputFormat input = new JDBCInputFormat.JDBCInputFormatBuilder()
            .setDrivername("com.mysql.cj.jdbc.Driver")
            .setUsername("root")
            .setPassword("123456")
            .setDBUrl("jdbc:mysql://localhost:3306/test_maxwell?serverTimezone=GMT%2b8")
            .setQuery("select * from test_flink")
            //设置获取的数据的类型
            .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO))
            .finish();

}
