package com.example.dto;
import com.delicloud.platform.v2.common.lang.bo.JsonBase;
import  lombok.Data;
import  lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class Info extends JsonBase {

  private  String name;

  private  Long age;

  private  String phone;

}
