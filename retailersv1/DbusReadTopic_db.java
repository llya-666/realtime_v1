package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.ProcessSpiltStreamToHBaseDim;
import com.retailersv1.func.ProcessSpiltStreamToKafkaDim;
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
 * mysql db cdc to kafka realtime_db topic
 */
public class DbusReadTopic_db {
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    private static final String  CDH_MYSQL_DATABASE= ConfigUtils.getString("mysql.database");
    private static final String  CDH_MYSQL_DATABASE_CONF= ConfigUtils.getString("mysql.databases.conf");
    private static final String  CDH_MYSQL_USER= ConfigUtils.getString("mysql.user");
    private static final String  CDH_MYSQL_PWD= ConfigUtils.getString("mysql.pwd");
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取mysql的数据
        MySqlSource<String> mySQLCdcSource = CdcSourceUtils.getMySQLCdcSource(
                CDH_MYSQL_DATABASE,
                "",
                CDH_MYSQL_USER,
                CDH_MYSQL_PWD,
                StartupOptions.earliest()
        );
        DataStreamSource<String> cdcMysqlMain = env.fromSource(mySQLCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");
        //     cdcMysqlMain.print("业务表");
        
        //配置表
        MySqlSource<String> mySQLCdcConf = CdcSourceUtils.getMySQLCdcSource(
                CDH_MYSQL_DATABASE_CONF,
                "gmall_conf.table_process_dwd",
                CDH_MYSQL_USER,
                CDH_MYSQL_PWD,
                StartupOptions.initial()
        );
        DataStreamSource<String> cdcMysqlConf = env.fromSource(mySQLCdcConf, WatermarkStrategy.noWatermarks(), "mysql_cdc_dim_source");
        //        cdcMysqlConf.print("配置表");

        //业务转换成json
        SingleOutputStreamOperator<JSONObject> cdcMysqlMainMap = cdcMysqlMain.map(JSONObject::parseObject);
//        cdcMysqlMainMap.print("业务表");

        //配置转换成json
        SingleOutputStreamOperator<JSONObject> cdcMsqlConfMap = cdcMysqlConf.map(JSONObject::parseObject);
        cdcMsqlConfMap.print("配置表");
        // 清洗配置表
//        SingleOutputStreamOperator<JSONObject> clearCdcMsqlConfMap = cdcMsqlConfMap.map(s -> {
////                    System.out.println(s+"=====================>");
//                    s.remove("source");
//                    s.remove("transaction");
//                    JSONObject resJson = new JSONObject();
//                    if ("d" .equals(s.getString("op"))) {
//                        resJson.put("before", s.getJSONObject("before"));
//                    } else {
//                        resJson.put("after", s.getJSONObject("after"));
//                    }
//                    resJson.put("op", s.getString("op"));
//                    return resJson;
//                })
//                .uid("clean_json_column_map")
//                .name("clean_json_column_map");
//        clearCdcMsqlConfMap.print("配置表清洗后>>>");

//        MapStateDescriptor<String, JSONObject> mapStageDesc = new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);
//        //创建广播流
//        BroadcastStream<JSONObject> broadcastDs = cdcMsqlConfMap.broadcast(mapStageDesc);
//        //连接
//        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = cdcMysqlMainMap.connect(broadcastDs);
//
//        connectDs.process(new ProcessSpiltStreamToKafkaDim(mapStageDesc));





        env.execute();
    }
}
