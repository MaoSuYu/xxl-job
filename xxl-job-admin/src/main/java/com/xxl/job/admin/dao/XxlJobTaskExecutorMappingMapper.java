package com.xxl.job.admin.dao;

import com.xxl.job.admin.core.model.XxlJobTaskExecutorMapping;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * xxl_job_task_executor_mapping表的Mapper接口
 */
@Mapper
public interface XxlJobTaskExecutorMappingMapper {

    /**
     * 根据任务ID查询执行器地址
     */
    String loadExecutorAddress(@Param("jobId") int jobId);

    /**
     * 保存或更新任务执行器映射
     */
    int saveOrUpdate(XxlJobTaskExecutorMapping mapping);

    /**
     * 根据任务ID删除映射
     */
    int deleteByJobId(@Param("jobId") int jobId);
} 