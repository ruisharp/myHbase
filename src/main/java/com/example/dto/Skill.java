package com.example.dto;

import com.delicloud.platform.v2.common.lang.bo.JsonBase;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class Skill   extends JsonBase {

  private  String major;

  private  String foreign;

}
