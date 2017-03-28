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

  /**
   * In this example Flink will read from the table `data` present in Database`source` and will
   * write in `mydata` table present in Database `sink`. We are using MySQL database for this
   * example.
   * Pre-requiste:
   * Two Database: Source and Sink
   * `Source` with table `data` from which we have to copy data.
   * `Sink` with table `mydata` in which we have to copy data.
   */
  public static void main(String[] args) throws Exception {

    final TypeInformation<?>[] fieldTypes =
        new TypeInformation<?>[] { BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO };

    final RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

    //Creates an execution environment that represents the context in which the program is currently executed.
    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

    String selectQuery = "select * from Student";
    String driverName = "com.mysql.cj.jdbc.Driver";
    String sourceDB = "source";
    String sinkDB = "sink";
    String dbURL = "jdbc:mysql://localhost:3306/";
    String dbPassword = "root";
    String dbUser = "root";

    //Define Input Format Builder
    JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
        JDBCInputFormat.buildJDBCInputFormat().setDrivername(driverName).setDBUrl(dbURL + sourceDB)
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
    String insertQuery = "INSERT INTO mydata (id, name) values (?, ?)";
    JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
        JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(driverName).setDBUrl(dbURL + sinkDB)
            .setQuery(insertQuery).setSqlTypes(new int[] { Types.INTEGER, Types.VARCHAR })
            .setUsername(dbUser).setPassword(dbPassword);

    //Define DataSink
    source.output(outputBuilder.finish());

    //Triggers the program execution.
    environment.execute();

  }

}
