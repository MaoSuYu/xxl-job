package com.xxl.job.admin.dao;

import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobShardingInfo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * 作者: Mr.Z
 * 时间: 2025-03-05 21:34
 */
@Mapper
public interface XxlJobShardingInfoDao {
    public int save(XxlJobShardingInfo info);

    int bathSave(@Param("list") List<XxlJobShardingInfo> xxlJobShardingInfos);

    int deleteByParentId(@Param("id") Long id);

    List<XxlJobShardingInfo> findListByIds(@Param("ids") List<String> shardingJobIds);

}
