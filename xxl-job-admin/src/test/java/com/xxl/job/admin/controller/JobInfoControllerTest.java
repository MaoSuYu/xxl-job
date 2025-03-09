package com.xxl.job.admin.controller;

import cn.hutool.core.util.IdUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.xxl.job.admin.core.util.TimeConverterUtil;
import com.xxl.job.admin.service.impl.LoginService;
import com.xxl.job.admin.service.impl.XxlJobServiceImpl;
import com.xxl.job.core.biz.model.HandleShardingParam;
import com.xxl.job.core.enums.TimeUnit;
import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.ObjectUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

public class JobInfoControllerTest extends AbstractSpringMvcTest {
  private static Logger logger = LoggerFactory.getLogger(JobInfoControllerTest.class);

  private Cookie cookie;

  @BeforeEach
  public void login() throws Exception {
    MvcResult ret = mockMvc.perform(
            post("/login")
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .param("userName", "admin")
                    .param("password", "123456")
    ).andReturn();
    cookie = ret.getResponse().getCookie(LoginService.LOGIN_IDENTITY_KEY);
  }

  @Test
  public void testAdd() throws Exception {
    MultiValueMap<String, String> parameters = new LinkedMultiValueMap<String, String>();
    parameters.add("jobGroup", "1");
    parameters.add("triggerStatus", "-1");

    MvcResult ret = mockMvc.perform(
            post("/jobinfo/pageList")
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    //.content(paramsJson)
                    .params(parameters)
                    .cookie(cookie)
    ).andReturn();

    logger.info(ret.getResponse().getContentAsString());
  }




  @Test
  public void testTrigger() throws Exception {
    XxlJobServiceImpl xxlJobServiceImpl = (XxlJobServiceImpl)SpringUtil.getBean("xxlJobServiceImpl");
    HandleShardingParam handleShardingParam = new HandleShardingParam();
    handleShardingParam.setSchedulingInterval(3);//调度间隔
    handleShardingParam.setSchedulingCycle(TimeUnit.MINUTE);// 调度的首次时间
    handleShardingParam.setFirstSchedulingTime("2023-10-05 14:30:45");// 数据的开始时间
    handleShardingParam.setSchedulingDeadline("2023-10-05 14:40:45");// 调度的截止时间
    handleShardingParam.setStartTimeOfData("2023-10-05 14:30:45");// 数据的截止时间
    handleShardingParam.setEndTimeOfData("2023-10-05 14:40:45");// 数据时间间隔
    handleShardingParam.setDataInterval(2);// 数据时间间隔
    handleShardingParam.setTimeUnit(TimeUnit.MINUTE);
    handleShardingParam.setAppName("xxl-job-executor-sample");// 执行器服务名称
    handleShardingParam.setId(1897554446039736320l);// 任务id
    handleShardingParam.setIsAutomatic(0);

    xxlJobServiceImpl.ShardingTrigger(handleShardingParam);
  }


  @Test
  public void testTriggerAutomatic() throws Exception {
    XxlJobServiceImpl xxlJobServiceImpl = (XxlJobServiceImpl)SpringUtil.getBean("xxlJobServiceImpl");
    HandleShardingParam handleShardingParam = new HandleShardingParam();
    handleShardingParam.setSchedulingInterval(1);//调度间隔
    handleShardingParam.setSchedulingCycle(TimeUnit.MINUTE);//
    handleShardingParam.setFirstSchedulingTime("2025-03-08 11:01:30");// 调度的首次时间
    handleShardingParam.setSchedulingDeadline("2025-03-08 11:04:30");// 调度的截止时间
    handleShardingParam.setStartTimeOfData("2023-10-05 14:30:45");// 数据的开始时间
    handleShardingParam.setEndTimeOfData("2023-10-05 14:40:45");// 数据时间间隔
    handleShardingParam.setDataInterval(1);// 数据时间间隔
    handleShardingParam.setTimeUnit(TimeUnit.MINUTE);
    handleShardingParam.setAppName("xxl-job-executor-sample");// 执行器服务名称
    handleShardingParam.setId(1897554446039736320l);// 任务id
    handleShardingParam.setIsAutomatic(1);

    xxlJobServiceImpl.ShardingTrigger(handleShardingParam);
  }

  @Test
  public void snowflakeNextId() throws Exception {

    System.out.println("IdUtil.getSnowflakeNextId() = " + IdUtil.getSnowflakeNextId());
  }

  @Test
  public void convertToTimestamp() throws Exception {
    long l = TimeConverterUtil.convertToTimestamp("2025-03-06 23:46:30");
    System.out.println("l : " + l);
  }

  @Test
  public void now() throws Exception {

    System.out.println("l : " + System.currentTimeMillis());
  }

  @Test
  public void testT() throws Exception {

    System.out.println("logger = " + TimeConverterUtil.calculateTimestamp(2, "MINUTE"));;
  }


  @Test
  public void testT1() throws Exception {
    Map<Long, List<Long>> ringData = new ConcurrentHashMap<>();
    List<Long> l = new ArrayList<>();
    l.add(1l);
    ringData.put(1l,l);
    List<Long> tmpData = ringData.remove(1l);
    System.out.println("tmpData = " + tmpData);
  }



}
