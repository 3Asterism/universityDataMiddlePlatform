package com.akisan.universityDataMiddlePlatform.mapper;

import com.akisan.universityDataMiddlePlatform.entity.std_scorebar;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface std_scorebarMapper {
    void deleteStdScoreAlarm();
}
