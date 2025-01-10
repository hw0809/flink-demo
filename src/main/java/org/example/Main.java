package org.example;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.enableCheckpointing(60000L);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("flink-demo") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("flink-demo.flink-data") // set captured table
                .username("root")
                .password("HW000089")
                .includeSchemaChanges(true)
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL-CDC")
                .print();
        env.execute();

    }
}