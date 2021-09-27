package peterchou.flink.table.basicTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import peterchou.flink.stream.beams.sensor.SensorData;
import peterchou.flink.stream.source.sourceFile.SourceFile.MyMapper;

public class BasicTable {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<String> data = env.readTextFile("../data/sensor.txt");
    SingleOutputStreamOperator<SensorData> sensorData = data.map(new MyMapper());

    // 创建表环境
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // 基于流创建表
    Table dataTable = tableEnv.fromDataStream(sensorData);

    // 调用table API进行转换操作
    // Table resultTable = dataTable.select("id, temperature").where("id ==
    // 'sensor_1'");

    // or使用flink SQL
    tableEnv.createTemporaryView("sensor", dataTable);
    String sql = "select id, temperature from sensor where id = 'sensor_1'";
    Table resultSqlTable = tableEnv.sqlQuery(sql);
    tableEnv.toAppendStream(resultSqlTable, Row.class).print("result");

    env.execute();
  }
}
