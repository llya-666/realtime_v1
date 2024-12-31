package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.MapUpdateHbaseDimTableFunc;
import com.retailersv1.func.ProcessSpiltStreamToHBaseDim;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import common.utils.ConfigUtils;
import common.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;




/**
 * @Package com.retailersv1.DbusCdc2KafkaTopic
 * @Author zhou.han
 * @Date 2024/12/12 12:56
 * @description: mysql db cdc to kafka realtime_db topic
 */
public class DbusCdc2KafkaTopic {

    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        //读取mysql数据库
        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
        MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "gmall_conf.table_process_dim",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

        DataStreamSource<String> cdcDbMainStream = env.fromSource(mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");
        cdcDbMainStream.print("业务表");

        DataStreamSource<String> cdcDbDimStream = env.fromSource(mySQLCdcDimConfSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dim_source");
//        cdcDbDimStream.print("配置表1>>>>");


        SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = cdcDbMainStream.map(JSONObject::parseObject)
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1);


        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMap = cdcDbDimStream.map(JSONObject::parseObject)
                .uid("dim_data_convert_json")
                .name("dim_data_convert_json")
                .setParallelism(1);
//        cdcDbDimStreamMap.print("配置表2>>>>");

        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMapCleanColumn = cdcDbDimStreamMap.map(s -> {
                    s.remove("source");
                    s.remove("transaction");
                    JSONObject resJson = new JSONObject();
                    if ("d".equals(s.getString("op"))){
                        resJson.put("before",s.getJSONObject("before"));
                    }else {
                        resJson.put("after",s.getJSONObject("after"));
                    }
                    resJson.put("op",s.getString("op"));
                    return resJson;
                }).uid("clean_json_column_map")
                .name("clean_json_column_map");
//        cdcDbDimStreamMapCleanColumn.print("配置表3>>>>");

        //Hbase中建表
        SingleOutputStreamOperator<JSONObject> tpDS = cdcDbDimStreamMapCleanColumn.map(new MapUpdateHbaseDimTableFunc(CDH_ZOOKEEPER_SERVER, CDH_HBASE_NAME_SPACE))
                .uid("map_create_hbase_dim_table")
                .name("map_create_hbase_dim_table");

        MapStateDescriptor<String, JSONObject> mapStageDesc = new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);
    //广播
        BroadcastStream<JSONObject> broadcastDs = tpDS.broadcast(mapStageDesc);
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = cdcDbMainStreamMap.connect(broadcastDs);
        connectDs.process(new ProcessSpiltStreamToHBaseDim(mapStageDesc));

//        cdcDbMainStream.sinkTo(
//              KafkaUtils.buildKafkaSink(ConfigUtils.getString("kafka.bootstrap.servers"),"kafka.topic.db")
//        ).uid("sink_to_kafka_realtime_v1_mysql_db").name("sink_to_kafka_realtime_v1_mysql_db");

        env.execute();
    }

}