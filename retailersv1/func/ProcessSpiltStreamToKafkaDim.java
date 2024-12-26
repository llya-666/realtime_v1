package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.TableProcessDim;
import common.utils.ConfigUtils;
import common.utils.JdbcUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

public class ProcessSpiltStreamToKafkaDim extends BroadcastProcessFunction<JSONObject,JSONObject,JSONObject> {
    private MapStateDescriptor<String, JSONObject> mapStateDescriptor;
    private HashMap<String, TableProcessDim> configMap = new HashMap<>();

    private static final String  CDH_MYSQL_USER= ConfigUtils.getString("mysql.user");
    private static final String  CDH_MYSQL_PWD= ConfigUtils.getString("mysql.pwd");
    private static final String  CDH_MYSQL_URL= ConfigUtils.getString("mysql.url");


    public void open(Configuration parameters) throws Exception {
        Connection connection = JdbcUtils.getMySQLConnection(
                CDH_MYSQL_URL,
                CDH_MYSQL_USER,
                CDH_MYSQL_PWD
        );
        String querySQL="select * from gmall_conf.table_process_dwd";
        List<TableProcessDim> tableProcessDims = JdbcUtils.queryList(connection, querySQL, TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        connection.close();
    }
    public ProcessSpiltStreamToKafkaDim(MapStateDescriptor<String, JSONObject> mapStageDesc) {
        this.mapStateDescriptor = mapStageDesc;
    }

    @Override
    public void processElement(JSONObject jsonObject,
                               BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext readOnlyContext,
                               Collector<JSONObject> collector) throws Exception {




    }

    @Override
    public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String op = jsonObject.getString("op");
        if(jsonObject.containsKey("after")){
            String sourceTableName = jsonObject.getJSONObject("after").getString("source_table");
            if("d".equals(op)){
                broadcastState.remove(sourceTableName);
            }else {
                broadcastState.put(sourceTableName,jsonObject);
            }

        }
    }
}
