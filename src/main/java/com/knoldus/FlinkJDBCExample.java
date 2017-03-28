package com.knoldus;

import java.sql.Types;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class FlinkJDBCExample {

  public static void main(String[] args) throws Exception {

    final TypeInformation<?>[] fieldTypes =
        new TypeInformation<?>[] { BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO };


    final RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

    //Creates an execution environment that represents the context in which the program is currently executed.
    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();


    String selectQuery = "select * from Student";
    String driverName = "com.mysql.cj.jdbc.Driver";
    String dbURL = "jdbc:mysql://localhost:3306/test";
    String dbPassword = "root";
    String dbUser = "root";

    //Define Input Format Builder
    JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
        JDBCInputFormat.buildJDBCInputFormat().setDrivername(driverName).setDBUrl(dbURL)
            .setQuery(selectQuery).setRowTypeInfo(rowTypeInfo).setUsername(dbUser)
            .setPassword(dbPassword);

    //Get Data from SQL Table
    DataSet<Row> source = environment.createInput(inputBuilder.finish());

    //Print DataSet
    source.print();

    //Transformation
    DataSet<Row> transformedSet = source.filter(row -> row.getField(1).toString().length() < 5);

    //Print Transformed DataSet
    transformedSet.print();

    //Define Output Format Builder
    String insertQuery = "INSERT INTO Student (id, name) values (?, ?)";
    JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
        JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(driverName).setDBUrl(dbURL)
            .setQuery(insertQuery).setSqlTypes(new int[] { Types.INTEGER, Types.VARCHAR })
            .setUsername(dbUser).setPassword(dbPassword);

    //Define DataSink
    source.output(outputBuilder.finish());

    //Triggers the program execution.
    environment.execute();

  }

}
