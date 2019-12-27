package com.example.table;

import java.util.Arrays;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class Relation {
  private static byte[] TABLE_NAME = Bytes.toBytes("ns_weibo:relation");

  private static byte[] F_ATTENDS = Bytes.toBytes("attends");

  private static byte[] F_FANS = Bytes.toBytes("fans");



  public static TableName getTableName() {
    return TableName.valueOf(TABLE_NAME);
  }

  public static byte[] getAttendsFamily() {
    return Arrays.copyOf(F_ATTENDS, F_ATTENDS.length);
  }


  public static byte[] getFansFamily() {
    return Arrays.copyOf(F_FANS, F_FANS.length);
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
