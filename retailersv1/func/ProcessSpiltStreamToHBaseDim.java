package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.TableProcessDim;
import common.utils.ConfigUtils;
import common.utils.HbaseUtils;
import common.utils.JdbcUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.Checkpoint;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Connection;
import java.util.*;


/**
 * @Package com.retailersv1.func.ProcessSpiltStreamToHBaseDim
 * @Author zhou.han
 * @Date 2024/12/19 22:55
 * @description:
 */
public class ProcessSpiltStreamToHBaseDim extends BroadcastProcessFunction<JSONObject,JSONObject,JSONObject> {

    private MapStateDescriptor<String, JSONObject> mapStateDescriptor;
    private HashMap<String, TableProcessDim> configMap = new HashMap<>();

    private org.apache.hadoop.hbase.client.Connection hbaseConnection ;
    private HbaseUtils hbaseUtils;
    private final String querySQL = "select * from gmall_conf.table_process_dim";

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));
        List<TableProcessDim> tableProcessDims = JdbcUtils.queryList(connection, querySQL, TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        connection.close();

        hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
        hbaseConnection = hbaseUtils.getConnection();

    }

    public ProcessSpiltStreamToHBaseDim(MapStateDescriptor<String, JSONObject> mapStageDesc) {
        this.mapStateDescriptor = mapStageDesc;
    }


    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //获取配置表的数据
        ReadOnlyBroadcastState<String, JSONObject> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String tableName = jsonObject.getJSONObject("source").getString("table");
        JSONObject broadData = broadcastState.get(tableName);
        // 这里可能为null NullPointerException
        if (broadData != null || configMap.get(tableName) != null){
            if (configMap.get(tableName).getSourceTable().equals(tableName)){
//                System.err.println(jsonObject);
                if (!jsonObject.getString("op").equals("d")){
                    JSONObject after = jsonObject.getJSONObject("after");
                    String sinkTableName = configMap.get(tableName).getSinkTable();
                    sinkTableName = "realtime_dev1:"+sinkTableName;
                    System.out.println("命名域加表名 - >:"+sinkTableName);
                    String hbaseRowKey = after.getString(configMap.get(tableName).getSinkRowKey());
                    Table hbaseConnectionTable = hbaseConnection.getTable(TableName.valueOf(sinkTableName));
                    Put put = new Put(Bytes.toBytes(hbaseRowKey));
                    for (Map.Entry<String, Object> entry : after.entrySet()) {
                        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(entry.getKey()),Bytes.toBytes(String.valueOf(entry.getValue())));
                    }
                    hbaseConnectionTable.put(put);
                    System.err.println("put -> "+put.toJSON()+" "+ Arrays.toString(put.getRow()));
                }
            }
        }
    }

    @Override
    public void processBroadcastElement(JSONObject jsonObject,
                                        BroadcastProcessFunction<JSONObject, JSONObject,
                                                JSONObject>.Context context,
                                        Collector<JSONObject> collector) throws Exception {

        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String op = jsonObject.getString("op");
        if (jsonObject.containsKey("after")) {
            String sourceTableName = jsonObject.getJSONObject("after").getString("source_table");
            if ("d".equals(op)) {
                broadcastState.remove(sourceTableName);
            } else {
                broadcastState.put(sourceTableName, jsonObject);
//               configMap.put(sourceTableName,jsonObject.toJavaObject(TableProcessDim.class));
            }
        }
    }

    //关闭
    @Override
    public void close() throws Exception {
        super.close();
        hbaseConnection.close();
    }



}
