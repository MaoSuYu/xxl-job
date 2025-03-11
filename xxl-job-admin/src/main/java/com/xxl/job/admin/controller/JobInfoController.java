package com.xxl.job.admin.controller;

import com.xxl.job.admin.controller.annotation.PermissionLimit;
import com.xxl.job.admin.controller.interceptor.PermissionInterceptor;
import com.xxl.job.admin.core.exception.XxlJobException;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobTaskExecutorMapping;
import com.xxl.job.admin.core.model.XxlJobUser;
import com.xxl.job.admin.core.route.ExecutorRouteStrategyEnum;
import com.xxl.job.admin.core.scheduler.MisfireStrategyEnum;
import com.xxl.job.admin.core.scheduler.ScheduleTypeEnum;
import com.xxl.job.admin.core.scheduler.XxlJobScheduler;
import com.xxl.job.admin.core.thread.JobScheduleHelper;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.admin.dao.XxlJobGroupDao;
import com.xxl.job.admin.dao.XxlJobTaskExecutorMappingMapper;
import com.xxl.job.admin.service.XxlJobService;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.KillParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.job.core.glue.GlueTypeEnum;
import com.xxl.job.core.util.DateUtil;
import jakarta.annotation.Resource;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.*;

/**
 * index controller
 * @author xuxueli 2015-12-19 16:13:16
 */
@Controller
@RequestMapping("/jobinfo")
public class JobInfoController {
	private static Logger logger = LoggerFactory.getLogger(JobInfoController.class);

	@Resource
	private XxlJobGroupDao xxlJobGroupDao;
	@Resource
	private XxlJobService xxlJobService;
	@Resource
	private XxlJobTaskExecutorMappingMapper xxlJobTaskExecutorMappingMapper;

	@RequestMapping
	public String index(HttpServletRequest request, Model model, @RequestParam(value = "jobGroup", required = false, defaultValue = "-1") int jobGroup) {

		// 枚举-字典
		model.addAttribute("ExecutorRouteStrategyEnum", ExecutorRouteStrategyEnum.values());	    // 路由策略-列表
		model.addAttribute("GlueTypeEnum", GlueTypeEnum.values());								// Glue类型-字典
		model.addAttribute("ExecutorBlockStrategyEnum", ExecutorBlockStrategyEnum.values());	    // 阻塞处理策略-字典
		model.addAttribute("ScheduleTypeEnum", ScheduleTypeEnum.values());	    				// 调度类型
		model.addAttribute("MisfireStrategyEnum", MisfireStrategyEnum.values());	    			// 调度过期策略

		// 执行器列表
		List<XxlJobGroup> jobGroupList_all =  xxlJobGroupDao.findAll();

		// filter group
		List<XxlJobGroup> jobGroupList = PermissionInterceptor.filterJobGroupByRole(request, jobGroupList_all);
		if (jobGroupList==null || jobGroupList.size()==0) {
			throw new XxlJobException(I18nUtil.getString("jobgroup_empty"));
		}

		model.addAttribute("JobGroupList", jobGroupList);
		model.addAttribute("jobGroup", jobGroup);

		return "jobinfo/jobinfo.index";
	}

	@RequestMapping("/pageList")
	@ResponseBody
	public Map<String, Object> pageList(@RequestParam(value = "start", required = false, defaultValue = "0") int start,
										@RequestParam(value = "length", required = false, defaultValue = "10") int length,
										@RequestParam("jobGroup") int jobGroup,
										@RequestParam("triggerStatus") int triggerStatus,
										@RequestParam("jobDesc") String jobDesc,
										@RequestParam("executorHandler") String executorHandler,
										@RequestParam("author") String author) {

		return xxlJobService.pageList(start, length, jobGroup, triggerStatus, jobDesc, executorHandler, author);
	}

	@RequestMapping("/add")
	@ResponseBody
	public ReturnT<String> add(HttpServletRequest request, XxlJobInfo jobInfo) {
		// valid permission
		PermissionInterceptor.validJobGroupPermission(request, jobInfo.getJobGroup());

		// opt
		XxlJobUser loginUser = PermissionInterceptor.getLoginUser(request);
		return xxlJobService.add(jobInfo, loginUser);
	}

	@RequestMapping("/update")
	@ResponseBody
	public ReturnT<String> update(HttpServletRequest request, XxlJobInfo jobInfo) {
		// valid permission
		PermissionInterceptor.validJobGroupPermission(request, jobInfo.getJobGroup());

		// opt
		XxlJobUser loginUser = PermissionInterceptor.getLoginUser(request);
		return xxlJobService.update(jobInfo, loginUser);
	}

	@RequestMapping("/remove")
	@ResponseBody
	public ReturnT<String> remove(@RequestParam("id") Long id) {
		return xxlJobService.remove(id);
	}

	@RequestMapping("/stop")
	@ResponseBody
	public ReturnT<String> pause(@RequestParam("id") Long id) {
		return xxlJobService.stop(id);
	}

	@RequestMapping("/start")
	@ResponseBody
	public ReturnT<String> start(@RequestParam("id") Long id) {
		return xxlJobService.start(id);
	}

	/**
	 * 手动触发任务的接口。
	 *
	 * @param request        HTTP请求对象，用于获取请求上下文。
	 * @param id             任务的唯一标识符，用于指定要触发的任务。
	 * @param executorParam  执行参数，传递给任务执行器。
	 * @param addressList    执行器地址列表，用于覆盖默认的执行器地址。
	 * @return               返回任务触发的结果，包含成功或失败的信息。
	 */
	@RequestMapping("/trigger")
	@ResponseBody
	public ReturnT<String> triggerJob(HttpServletRequest request,
									  @RequestParam("id") Long id,
									  @RequestParam("executorParam") String executorParam,
									  @RequestParam("addressList") String addressList) {

		// 获取当前登录用户的信息
		XxlJobUser loginUser = PermissionInterceptor.getLoginUser(request);
		// 调用服务层的trigger方法，执行任务触发逻辑
		return xxlJobService.trigger(loginUser, id, executorParam, addressList);
	}

	@RequestMapping("/nextTriggerTime")
	@ResponseBody
	public ReturnT<List<String>> nextTriggerTime(@RequestParam("scheduleType") String scheduleType,
												 @RequestParam("scheduleConf") String scheduleConf) {

		XxlJobInfo paramXxlJobInfo = new XxlJobInfo();
		paramXxlJobInfo.setScheduleType(scheduleType);
		paramXxlJobInfo.setScheduleConf(scheduleConf);

		List<String> result = new ArrayList<>();
		try {
			Date lastTime = new Date();
			for (int i = 0; i < 5; i++) {
				lastTime = JobScheduleHelper.generateNextValidTime(paramXxlJobInfo, lastTime);
				if (lastTime != null) {
					result.add(DateUtil.formatDateTime(lastTime));
				} else {
					break;
				}
			}
		} catch (Exception e) {
			logger.error("nextTriggerTime error. scheduleType = {}, scheduleConf= {}", scheduleType, scheduleConf, e);
			return new ReturnT<List<String>>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")+I18nUtil.getString("system_unvalid")) + e.getMessage());
		}
		return new ReturnT<List<String>>(result);

	}

	@RequestMapping("/forceKill")
	@ResponseBody
	public ReturnT<String> forceKill(@RequestParam("id") Long id) {
		// 1. 先停止调度
		ReturnT<String> stopResult = xxlJobService.stop(id);
		if (stopResult.getCode() != ReturnT.SUCCESS_CODE) {
			return stopResult;
		}

		// 2. 从映射表中获取执行节点
		String executorAddress = xxlJobTaskExecutorMappingMapper.loadExecutorAddress(id);
		if (executorAddress == null) {
			return new ReturnT<>(ReturnT.FAIL_CODE, "未找到任务执行节点");
		}

		// 3. 调用执行器的强制打断接口
		try {
			ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(executorAddress);
			return executorBiz.forceKill(id);
		} catch (Exception e) {
			logger.error("强制打断任务失败 [任务ID:{}] [执行器:{}] [异常:{}]", id, executorAddress, e.getMessage());
			return new ReturnT<>(ReturnT.FAIL_CODE, "强制打断任务失败：" + e.getMessage());
		}
	}

	@GetMapping("/offline")
	@ResponseBody
	public ReturnT<String> offline(@RequestParam("ip") String ip) {
		try {
			// 获取执行器客户端
			ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(ip);
			if (executorBiz == null) {
				return new ReturnT<>(ReturnT.FAIL_CODE, "获取执行器客户端失败，执行器可能已离线");
			}
			
			// 调用执行器的offline方法
			return executorBiz.offline(ip);
		} catch (Exception e) {
			logger.error("执行器下线失败 [执行器IP:{}] [异常:{}]", ip, e.getMessage());
			return new ReturnT<>(ReturnT.FAIL_CODE, "执行器下线失败：" + e.getMessage());
		}
	}

}
