package com.example.controller;

import com.delicloud.platform.v2.common.lang.bo.RespBase;
import com.delicloud.platform.v2.common.lang.util.JacksonUtil;
import com.example.config.Hbase;
import com.example.dto.UserDto;
import com.example.table.Relation;
import com.example.table.User;
import com.example.util.SnowFlake;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/weibo")
@Slf4j
public class WeiBoController {
  @Autowired
  Hbase hbase;


  @RequestMapping("/saveUser")
  public RespBase<Long> saveUser(@RequestBody UserDto userDto) throws Exception {
    Connection hbaseConnect = hbase.getHbaseConnect();
      try {
      Table table = hbaseConnect.getTable(User.getTableName());
      Long rowKey = SnowFlake.nextId();
      userDto.setId(Long.valueOf(rowKey));
      log.info(rowKey+"");
      Put put = new Put(User.getRowKey(rowKey+""));
      put.addColumn(User.getInfoFamily(), User.getRowKey(rowKey+""), Bytes.toBytes(userDto.toString()));
      table.put(put);
      return new RespBase<>(rowKey);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }
  @RequestMapping("/addAttend")
  public RespBase<Void> addAttend(@RequestParam(required = true) Long uid,@RequestParam(required = true) Long attendId)
      throws IOException {
    Connection hbaseConnect = hbase.getHbaseConnect();
    try {
      Table table = hbaseConnect.getTable(Relation.getTableName());
      Put put = new Put(Relation.getRowKey(String.valueOf(uid)));
      put.addColumn(Relation.getAttendsFamily() , Bytes.toBytes(attendId), Bytes.toBytes(attendId));
      Put putFans = new Put(Relation.getRowKey(String.valueOf(attendId)));
      putFans.addColumn(Relation.getFansFamily() , Bytes.toBytes(uid) , Bytes.toBytes(uid));
      List<Put> putList = new ArrayList<>();
      putList.add(put);
      putList.add(putFans);
      table.put(putList);
      return  RespBase.OK_RESP_BASE;
    }catch (IOException e){
      e.printStackTrace();
      throw e;
    }
  }
  @RequestMapping("/listAttend")
  public  RespBase<List<UserDto>> listAttend(@RequestParam(required = true)Long uid) throws IOException {
    Connection hbaseConnect = hbase.getHbaseConnect();
    try{
      Table table = hbaseConnect.getTable(Relation.getTableName());
      Get get = new Get(Relation.getRowKey(String.valueOf(uid)));
      get.addFamily(Relation.getAttendsFamily());
      List<UserDto>  userDtos = new ArrayList<>();
      Result result = table.get(get);
      Table tableUser = hbaseConnect.getTable(User.getTableName());
/*      for(Map.Entry<byte[], byte[]> entry : result.getFamilyMap(Bytes.toBytes("attends")).entrySet()) {
        log.info("{}, {}", Bytes.toLong(entry.getKey()), Bytes.toLong(entry.getValue()));
        Get getUser = new Get(Bytes.toBytes(Bytes.toLong(entry.getKey())+""));
        //getUser.addColumn(User.getInfoFamily() , cell.getValueArray());
        Result resultUser = tableUser.get(getUser);
        Cell cellUser = resultUser.getColumnLatestCell(User.getInfoFamily() , Bytes.toBytes(Bytes.toLong(entry.getKey())+""));
        //String value = Bytes.toString(cellUser.getValueArray(), cellUser.getValueOffset(), cellUser.getValueLength());
        byte [] value = resultUser.getValue(User.getInfoFamily() , Bytes.toBytes(Bytes.toLong(entry.getKey())+""));
        String str= new String (value , "UTF-8");
        UserDto userDto = JacksonUtil.json2Bean(str , UserDto.class);
        userDtos.add(userDto);
      }*/

      for (Cell cell : result.rawCells()){
        log.info(Bytes.toLong(CellUtil.cloneValue(cell))+"");
        log.info(Bytes.toLong(cell.getValueArray() , cell.getValueOffset(), cell.getValueLength())+"");
        Get getUser = new Get(Bytes.toBytes(Bytes.toLong(CellUtil.cloneValue(cell))+""));
        //getUser.addColumn(User.getInfoFamily() , Bytes.toBytes(Bytes.toLong(CellUtil.cloneValue(cell))));
        Result resultUser = tableUser.get(getUser);
         Cell cellUser = resultUser.getColumnLatestCell(User.getInfoFamily() , Bytes.toBytes(Bytes.toLong(CellUtil.cloneValue(cell))+""));
        //String value = Bytes.toString(cellUser.getValueArray(), cellUser.getValueOffset(), cellUser.getValueLength());
       // byte [] value = resultUser.getValue(User.getInfoFamily() , cell.getValueArray());
        //String str= new String (value , "UTF-8");
        String str = Bytes.toString(CellUtil.cloneValue(cellUser));
        String string = Bytes.toString(cellUser.getValueArray() , cellUser.getValueOffset() , cellUser.getValueLength());
        log.info(str);log.info(string);
        UserDto userDto = JacksonUtil.json2Bean(str , UserDto.class);
        userDtos.add(userDto);
      }
      return  new RespBase<>(userDtos);
    }catch  (IOException e){
      e.printStackTrace();
      throw e;
    }
  }

}
