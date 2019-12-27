package com.example.dto;

import com.delicloud.platform.v2.common.lang.bo.JsonBase;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class UserDto  extends JsonBase {

  private  Long id;

  private  String name;

  private  String sex;

  private  String nickName;


}
