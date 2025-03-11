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

    int logicDeleteByParentId(@Param("id") Long id);

    List<XxlJobShardingInfo> findListByParentJobId(@Param("id") Long id,@Param("isAutomatic") Integer isAutomatic);

    int findMaxExecuteBatch(@Param("parentJobId") Long id);

    int updateExecuteInfo(@Param("state") int state,@Param("id") Long id);

    //int updateTriggerInfo(@Param("state") int state,@Param("id") Long id);
}
