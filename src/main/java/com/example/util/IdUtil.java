package com.example.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IdUtil {
  /**
   * 根据服务器IP生成workerId。
   * 具体代码来自：https://www.cnblogs.com/hongdada/p/9324473.html
   *
   * @return 唯一的workerId
   */
  public static long getWorkerId() throws IOException {
    InetAddress address;
    try {
      address = InetAddress.getLocalHost();
    } catch (final UnknownHostException e) {
      log.error("获取IP地址失败: {}", e.getMessage());
      throw new IllegalStateException("获取IP地址失败");
    }
    // 得到IP地址的byte[]形式值
    byte[] ipAddressByteArray = address.getAddress();
    long workerId = 0L;
    //如果是IPV4，计算方式是遍历byte[]，然后把每个IP段数值相加得到的结果就是workerId
    if (ipAddressByteArray.length == 4) {
      for (byte byteNum : ipAddressByteArray) {
        workerId += byteNum & 0xFF;
      }
      //如果是IPV6，计算方式是遍历byte[]，然后把每个IP段后6位（& 0B111111 就是得到后6位）数值相加得到的结果就是workerId
    } else if (ipAddressByteArray.length == 16) {
      for (byte byteNum : ipAddressByteArray) {
        workerId += byteNum & 0B111111;
      }
    } else {
      log.error("IP地址获取异常: {}", ipAddressByteArray);
      throw new IllegalStateException("IP地址获取异常：" + new String(ipAddressByteArray, "UTF-8"));
    }

    return workerId;
  }


}
