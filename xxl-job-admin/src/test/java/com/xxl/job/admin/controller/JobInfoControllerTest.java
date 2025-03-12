package com.xxl.job.admin.controller;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.xxl.job.admin.core.util.TimeConverterUtil;
import com.xxl.job.admin.service.impl.LoginService;
import com.xxl.job.admin.service.impl.XxlJobServiceImpl;
import com.xxl.job.core.biz.model.HandleShardingParam;
import com.xxl.job.core.enums.TimeUnit;
import com.xxl.job.core.util.DateUtil;
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

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
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
    handleShardingParam.setDataInterval(3);// 数据时间间隔
    handleShardingParam.setTimeUnit(TimeUnit.MINUTE);
    handleShardingParam.setAppName("vip-executor");// 执行器服务名称
    handleShardingParam.setId(1897554446039736320l);// 任务id
    handleShardingParam.setIsAutomatic(0);
    handleShardingParam.setExecuteHandle("demoJobHandler");

    xxlJobServiceImpl.ShardingTrigger(handleShardingParam);
    //java.util.concurrent.TimeUnit.SECONDS.sleep(30);
  }





  @Test
  public void testShardingTriggerWithMultipleTimeRanges() throws Exception {
    XxlJobServiceImpl xxlJobService = SpringUtil.getBean(XxlJobServiceImpl.class);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Calendar calendar = Calendar.getInstance();

    // 初始时间设置为示例中的开始时间
    calendar.set(2023, Calendar.OCTOBER, 5, 14, 30, 45);
    Date startTime = calendar.getTime();

    // 创建基础参数对象
    HandleShardingParam baseParam = createBaseParam();

    for (int i = 0; i < 10; i++) {
      // 创建新参数对象
      HandleShardingParam param = new HandleShardingParam();
      BeanUtil.copyProperties(baseParam,param);

      // 设置时间范围（每次递增10分钟）
      calendar.setTime(startTime);
      calendar.add(Calendar.MINUTE, i * 10);
      Date currentStart = calendar.getTime();

      calendar.add(Calendar.MINUTE, 10);
      Date currentEnd = calendar.getTime();

      param.setStartTimeOfData(sdf.format(currentStart));
      param.setEndTimeOfData(sdf.format(currentEnd));

      // 执行触发
      xxlJobService.ShardingTrigger(param);
    }
  }

  private HandleShardingParam createBaseParam() {
    HandleShardingParam param = new HandleShardingParam();
    param.setSchedulingInterval(3);
    param.setSchedulingCycle(TimeUnit.MINUTE);
    param.setFirstSchedulingTime("2023-10-05 14:30:45");
    param.setSchedulingDeadline("2023-10-05 14:40:45");
    param.setDataInterval(2);
    param.setTimeUnit(TimeUnit.MINUTE);
    param.setAppName("vip-executor");
    param.setId(1897554446039736320L);
    param.setIsAutomatic(0);
    param.setExecuteHandle("demoJobHandler");
    return param;
  }


  @Test
  public void testTriggerAutomatic() throws Exception {
    XxlJobServiceImpl xxlJobServiceImpl = (XxlJobServiceImpl)SpringUtil.getBean("xxlJobServiceImpl");
    HandleShardingParam handleShardingParam = new HandleShardingParam();
    handleShardingParam.setSchedulingInterval(1);//调度间隔
    handleShardingParam.setSchedulingCycle(TimeUnit.MINUTE);//
    handleShardingParam.setFirstSchedulingTime("2025-03-12 15:02:30");// 调度的首次时间
    handleShardingParam.setSchedulingDeadline("2025-03-12 15:06:30");// 调度的截止时间
    handleShardingParam.setStartTimeOfData("2023-10-05 14:30:45");// 数据的开始时间
    handleShardingParam.setEndTimeOfData("2023-10-05 14:40:45");// 数据时间间隔
    handleShardingParam.setDataInterval(2);// 数据时间间隔
    handleShardingParam.setTimeUnit(TimeUnit.MINUTE);
    handleShardingParam.setAppName("normal");// 执行器服务名称
    handleShardingParam.setId(1897554446039736320l);// 任务id
    handleShardingParam.setIsAutomatic(1);
    handleShardingParam.setPriority(2);
    handleShardingParam.setExecuteHandle("demoJobHandler");

    xxlJobServiceImpl.ShardingTrigger(handleShardingParam);

  }

@Test
  public void testTriggerAutomatic2() throws Exception {
    XxlJobServiceImpl xxlJobServiceImpl = (XxlJobServiceImpl)SpringUtil.getBean("xxlJobServiceImpl");
    HandleShardingParam handleShardingParam = new HandleShardingParam();
    handleShardingParam.setSchedulingInterval(1);//调度间隔
    handleShardingParam.setSchedulingCycle(TimeUnit.MINUTE);//
    handleShardingParam.setFirstSchedulingTime("2025-03-11 12:52:30");// 调度的首次时间
    handleShardingParam.setSchedulingDeadline("2025-03-11 12:55:30");// 调度的截止时间
    handleShardingParam.setStartTimeOfData("2023-10-06 14:30:45");// 数据的开始时间
    handleShardingParam.setEndTimeOfData("2023-10-06 14:40:45");// 数据时间间隔
    handleShardingParam.setDataInterval(1);// 数据时间间隔
    handleShardingParam.setTimeUnit(TimeUnit.MINUTE);
    handleShardingParam.setAppName("normal");// 执行器服务名称
    handleShardingParam.setId(1897554446039736322l);// 任务id
    handleShardingParam.setIsAutomatic(1);
    handleShardingParam.setPriority(1);
  handleShardingParam.setExecuteHandle("demoJobHandler");

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
