package com.akisan.universityDataMiddlePlatform.entity;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class test_flink {
    @ApiModelProperty(value = "id")
    private Integer id;
    @ApiModelProperty(value = "name")
    private String name;
    @ApiModelProperty(value = "age")
    private Integer age;
}
