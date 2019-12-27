package com.example.dto;

import com.delicloud.platform.v2.common.lang.bo.JsonBase;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class Address  extends JsonBase {

  private  String provice;

  private  String city;

  private String street;

}
