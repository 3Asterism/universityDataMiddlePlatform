package com.akisan.universityDataMiddlePlatform.controller;

import com.akisan.universityDataMiddlePlatform.service.impl.reduceActvStreamImpl;
import com.akisan.universityDataMiddlePlatform.service.impl.reduceExamStreamImpl;
import com.akisan.universityDataMiddlePlatform.common.resultForRequestConstant;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/dataProcess")
@Api(tags = "数据处理模块")
public class dataProcess {
    @Autowired
    reduceExamStreamImpl reduceExamStreamImpl;
    @Autowired
    reduceActvStreamImpl reduceActvStreamImpl;

    @PostMapping("/processTestData")
    @ApiOperation(value = "处理考试数据")
    public resultForRequestConstant processTestData() throws Exception {
        reduceExamStreamImpl.reduceExamStream();
        return resultForRequestConstant.success();
    }

    @PostMapping("/processActvData")
    @ApiOperation(value = "处理活动数据")
    public resultForRequestConstant processActvData() throws Exception {
        reduceActvStreamImpl.reduceActvStream();
        return resultForRequestConstant.success();
    }

}
