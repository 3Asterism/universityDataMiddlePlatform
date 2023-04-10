package com.akisan.universityDataMiddlePlatform.entity;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@ApiModel(description = "学生 - 详细信息结果表")
@NoArgsConstructor
@AllArgsConstructor
public class std_info {
    @ApiModelProperty(value = "学生id")
    private Integer id;

    @ApiModelProperty(value = "学生姓名")
    private String name;

    @ApiModelProperty(value = "学生所在班级")
    private String classname;

    @ApiModelProperty(value = "学生是否在校 1是0否")
    private String inschool;

    @ApiModelProperty(value = "学生考试通过率")
    private String passrate;

    @ApiModelProperty(value = "学生活动参与率")
    private String actvrate;

}
