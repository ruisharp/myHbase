package com.example.controller;

import com.example.config.Hbase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

@RestController
public class HelloController {
    @Autowired
    Hbase hbase;

    static final String TABLE_NAME = "rui_test";


    @RequestMapping("/saveDeviceInfoToHbase")
    public Object saveDeviceInfoToHbase(@RequestParam(required = true) String info,
                                        @RequestParam(required = true) String address,
                                        @RequestParam(required = true) String skill
    ) throws Exception {
        String rowKey = DigestUtils.md5Hex(info + address + skill);
        this.updateTable(rowKey, "address", "address:provice", address);
        this.updateTable(rowKey, "skill", "chip", skill);
        HashMap<Object, Object> data = new HashMap<>();
        data.put("serviceId", rowKey);
        return data;
    }


    @RequestMapping("/getDeviceInfoHbase")
    public Object getInfoHbase(@RequestParam(required = true) String sid
    ) throws IOException {
        Connection hbaseConnect = hbase.getHbaseConnect();
        Table table = hbaseConnect.getTable(TableName.valueOf(TABLE_NAME));
        Get get = new Get(Bytes.toBytes(sid));
        get.addColumn(Bytes.toBytes("model"), Bytes.toBytes("model"));
        get.addColumn(Bytes.toBytes("chip"), Bytes.toBytes("chip"));
        get.addColumn(Bytes.toBytes("mac"), Bytes.toBytes("mac"));
        get.addColumn(Bytes.toBytes("emmcId"), Bytes.toBytes("emmcId"));
        get.addColumn(Bytes.toBytes("barcode"), Bytes.toBytes("barcode"));
        get.addColumn(Bytes.toBytes("tcVersion"), Bytes.toBytes("tcVersion"));
        get.addColumn(Bytes.toBytes("systemVersion"), Bytes.toBytes("systemVersion"));
        get.setMaxVersions(30);
        Result result = table.get(get);
        ArrayList<Object> objects = new ArrayList<>();
        if (result.listCells() != null) {
            for (Cell cell : result.listCells()) {
                HashMap<Object, Object> data = new HashMap<>();
                data.put("family", Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                data.put("qualifier", Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                data.put("value", Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                data.put("Timestamp", cell.getTimestamp());
                objects.add(data);
            }
        } else {
            return objects;
        }

        return objects;
    }


    public void updateTable(String rowKey, String familyName, String columnName, String value) throws Exception {

        Connection hbaseConnect = hbase.getHbaseConnect();
        try {
            Table table = hbaseConnect.getTable(TableName.valueOf(TABLE_NAME));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
            table.put(put);
            System.out.println("Update table success");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

