package com.example.table;

import java.util.Arrays;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class User {

  private static byte[] TABLE_NAME = Bytes.toBytes("ns_weibo:user");

  private static byte[] F_INFO = Bytes.toBytes("info");



  public static TableName getTableName() {
    return TableName.valueOf(TABLE_NAME);
  }

  public static byte[] getInfoFamily() {
    return Arrays.copyOf(F_INFO, F_INFO.length);
  }


  /**
   * 获取行健
   *
   * @param rowKey 消息唯一标示
   * @return 行健信息
   */
  public static byte[] getRowKey(String rowKey) {
    return Bytes.toBytes(rowKey);
  }

}
