package org.example;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);
        env.enableCheckpointing(60000L);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.39.245")
                .port(3306)
                .databaseList("flink-demo") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("flink-demo.flink_data") // set captured table
                .username("root")
                .password("HW000089")
                .serverTimeZone("Asia/Shanghai") // 指定 MySQL 服务器时区
                .includeSchemaChanges(true)
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        /*env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL-CDC")
                .print();
*/
        /*env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL-CDC")
                .map(record -> {
                    System.out.println("Received record: " + record);
                    return record;
                })
                .print(); // 将数据打印到标准输出

        env.execute();*/

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL-CDC")
                .map(record -> {
                    // 解析 JSON 数据
                    // 假设 record 是 JSON 格式，解析后返回一个数组或自定义对象

                    // 解析 JSON 数据
                    String name = "emptyData";
                    JsonObject beforeNode = null;
                    if (!StringUtils.isBlank(record)) {
                        JsonObject rootNode = JsonParser.parseString(record).getAsJsonObject();
                        beforeNode = rootNode.getAsJsonObject("before");
                    }

                    if (beforeNode != null) {
                        // 提取字段
                        int id = beforeNode.get("id").getAsInt();
                        name = beforeNode.get("name").getAsString();

                        System.out.println("Received Record: ID=" + id + ", Name=" + name);

                        // 返回自定义对象或其他内容
//                        return String.format("ID=%d, Name=%s, Value=%d", id, name, value);
                    }

                    return new String[]{name}; // 替换为实际逻辑
                })
                .addSink(new MySQLSink());

        env.execute("MySQL CDC to MySQL Sink");
    }

    public static class MySQLSink extends RichSinkFunction<String[]> {
        private Connection connection;
        private PreparedStatement statement;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            // 配置目标数据库连接
            String url = "jdbc:mysql://192.168.39.245:3306/flink-demo?serverTimezone=Asia/Shanghai";
            String username = "root";
            String password = "HW000089";
            connection = DriverManager.getConnection(url, username, password);

            // 准备 SQL 语句
            String sql = "INSERT INTO flink_data_copy (name) VALUES (?)";
            statement = connection.prepareStatement(sql);
        }

        @Override
        public void invoke(String[] value, Context context) throws Exception {
            // 设置 SQL 参数
            statement.setString(1, value[0]);

            // 执行 SQL
            statement.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}