package peterchou.flink.stream.sink.mysqlSink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import peterchou.flink.stream.beams.sensor.SensorData;
import peterchou.flink.stream.source.sourceFile.SourceFile.MyMapper;

public class MysqlSink {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<String> data = env.readTextFile("../data/sensor.txt");

    DataStream<SensorData> sensorData = data.map(new MyMapper());

    sensorData.addSink(new SensorDataJdbcSink());

    env.execute();
  }
}

class SensorDataJdbcSink extends RichSinkFunction<SensorData> {
  @Override
  public void close() throws Exception {
    insertStmt.close();
    updateStmt.close();
    connection.close();
  }

  Connection connection = null;
  PreparedStatement insertStmt = null;
  PreparedStatement updateStmt = null;

  @Override
  public void open(Configuration parameters) throws Exception {
    // TODO Auto-generated method stub
    super.open(parameters);
    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "password");
    insertStmt = connection.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)");
    updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
  }

  @Override
  public void invoke(SensorData value, Context context) throws Exception {
    // TODO Auto-generated method stub
    // super.invoke(value, context);
    updateStmt.setDouble(1, value.getTemperature());
    updateStmt.setString(2, value.getId());
    updateStmt.execute();
    if (updateStmt.getUpdateCount() == 0) {

      insertStmt.setString(1, value.getId());
      insertStmt.setDouble(2, value.getTemperature());
      insertStmt.execute();
    }
  }

}
