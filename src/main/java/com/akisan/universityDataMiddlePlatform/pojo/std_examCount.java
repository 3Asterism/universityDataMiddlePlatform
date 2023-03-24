package com.akisan.universityDataMiddlePlatform.pojo;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class std_examCount {
    @ApiModelProperty(value = "考试信息id")
    private Integer id;

    @ApiModelProperty(value = "学生id")
    private Integer stdid;

    @ApiModelProperty(value = "学生姓名")
    private String name;

    @ApiModelProperty(value = "考试课程")
    private String examclass;

    @ApiModelProperty(value = "考试分数")
    private Integer score;

    @ApiModelProperty(value = "计数")
    private Integer count = 0;
}
