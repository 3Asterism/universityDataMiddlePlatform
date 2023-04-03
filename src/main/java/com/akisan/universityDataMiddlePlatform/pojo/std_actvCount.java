package com.akisan.universityDataMiddlePlatform.pojo;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class std_actvCount {

    @ApiModelProperty(value = "主键id")
    private Integer id;

    @ApiModelProperty(value = "学生id")
    private Integer stdid;

    @ApiModelProperty(value = "学生姓名")
    private String name;

    @ApiModelProperty(value = "活动名称")
    private String actvname;

    @ApiModelProperty(value = "应参加人数")
    private Integer attempt;

    @ApiModelProperty(value = "计数")
    private Integer count;
}
