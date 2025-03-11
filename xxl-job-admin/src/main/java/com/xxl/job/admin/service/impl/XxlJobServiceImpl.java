package com.xxl.job.admin.service.impl;

import cn.hutool.core.util.IdUtil;
import cn.hutool.json.JSONUtil;
import com.xuxueli.springbootpriorityqueue.model.SortedTask;
import com.xuxueli.springbootpriorityqueue.model.Task;
import com.xuxueli.springbootpriorityqueue.service.SortedTaskService;
import com.xuxueli.springbootpriorityqueue.service.TaskService;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.cron.CronExpression;
import com.xxl.job.admin.core.model.*;
import com.xxl.job.admin.core.route.ExecutorRouteStrategyEnum;
import com.xxl.job.admin.core.scheduler.MisfireStrategyEnum;
import com.xxl.job.admin.core.scheduler.ScheduleTypeEnum;
import com.xxl.job.admin.core.thread.JobScheduleHelper;
import com.xxl.job.admin.core.thread.JobTriggerPoolHelper;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.util.*;
import com.xxl.job.admin.dao.*;
import com.xxl.job.admin.service.XxlJobService;
import com.xxl.job.core.biz.model.HandleShardingParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.ExecutionStatus;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ObjectUtils;
import com.xxl.job.core.glue.GlueTypeEnum;
import com.xxl.job.core.util.DateUtil;
import com.xxl.job.core.enums.TimeUnit;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.temporal.ChronoUnit;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * core job action for xxl-job
 * @author xuxueli 2016-5-28 15:30:33
 */
@Service
public class XxlJobServiceImpl implements XxlJobService {
	private static Logger logger = LoggerFactory.getLogger(XxlJobServiceImpl.class);

	@Resource
	private XxlJobGroupDao xxlJobGroupDao;
	@Resource
	private XxlJobInfoDao xxlJobInfoDao;
	@Resource
	public XxlJobLogDao xxlJobLogDao;
	@Resource
	private XxlJobLogGlueDao xxlJobLogGlueDao;
	@Resource
	private XxlJobLogReportDao xxlJobLogReportDao;

	@Resource
	private TaskService taskService;

	@Resource
	private SortedTaskService sortedTaskService;

	@Override
	public Map<String, Object> pageList(int start, int length, int jobGroup, int triggerStatus, String jobDesc, String executorHandler, String author) {

		// page list
		List<XxlJobInfo> list = xxlJobInfoDao.pageList(start, length, jobGroup, triggerStatus, jobDesc, executorHandler, author);
		int list_count = xxlJobInfoDao.pageListCount(start, length, jobGroup, triggerStatus, jobDesc, executorHandler, author);

		// package result
		Map<String, Object> maps = new HashMap<String, Object>();
	    maps.put("recordsTotal", list_count);		// 总记录数
	    maps.put("recordsFiltered", list_count);	// 过滤后的总记录数
	    maps.put("data", list);  					// 分页列表
		return maps;
	}

	@Override
	public ReturnT<String> add(XxlJobInfo jobInfo, XxlJobUser loginUser) {

		// valid base
		XxlJobGroup group = xxlJobGroupDao.load(jobInfo.getJobGroup());
		if (group == null) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_choose")+I18nUtil.getString("jobinfo_field_jobgroup")) );
		}
		if (jobInfo.getJobDesc()==null || jobInfo.getJobDesc().trim().length()==0) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input")+I18nUtil.getString("jobinfo_field_jobdesc")) );
		}
		if (jobInfo.getAuthor()==null || jobInfo.getAuthor().trim().length()==0) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input")+I18nUtil.getString("jobinfo_field_author")) );
		}

		// valid trigger
		ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
		if (scheduleTypeEnum == null) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")+I18nUtil.getString("system_unvalid")) );
		}
		if (scheduleTypeEnum == ScheduleTypeEnum.CRON) {
			if (jobInfo.getScheduleConf()==null || !CronExpression.isValidExpression(jobInfo.getScheduleConf())) {
				return new ReturnT<String>(ReturnT.FAIL_CODE, "Cron"+I18nUtil.getString("system_unvalid"));
			}
		} else if (scheduleTypeEnum == ScheduleTypeEnum.FIX_RATE/* || scheduleTypeEnum == ScheduleTypeEnum.FIX_DELAY*/) {
			if (jobInfo.getScheduleConf() == null) {
				return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")) );
			}
			try {
				int fixSecond = Integer.valueOf(jobInfo.getScheduleConf());
				if (fixSecond < 1) {
					return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")+I18nUtil.getString("system_unvalid")) );
				}
			} catch (Exception e) {
				return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")+I18nUtil.getString("system_unvalid")) );
			}
		}

		// valid job
		if (GlueTypeEnum.match(jobInfo.getGlueType()) == null) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_gluetype")+I18nUtil.getString("system_unvalid")) );
		}
		if (GlueTypeEnum.BEAN==GlueTypeEnum.match(jobInfo.getGlueType()) && (jobInfo.getExecutorHandler()==null || jobInfo.getExecutorHandler().trim().length()==0) ) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input")+"JobHandler") );
		}
		// 》fix "\r" in shell
		if (GlueTypeEnum.GLUE_SHELL==GlueTypeEnum.match(jobInfo.getGlueType()) && jobInfo.getGlueSource()!=null) {
			jobInfo.setGlueSource(jobInfo.getGlueSource().replaceAll("\r", ""));
		}

		// valid advanced
		if (ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null) == null) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorRouteStrategy")+I18nUtil.getString("system_unvalid")) );
		}
		if (MisfireStrategyEnum.match(jobInfo.getMisfireStrategy(), null) == null) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("misfire_strategy")+I18nUtil.getString("system_unvalid")) );
		}
		if (ExecutorBlockStrategyEnum.match(jobInfo.getExecutorBlockStrategy(), null) == null) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorBlockStrategy")+I18nUtil.getString("system_unvalid")) );
		}

		// 》ChildJobId valid
		if (jobInfo.getChildJobId()!=null && jobInfo.getChildJobId().trim().length()>0) {
			String[] childJobIds = jobInfo.getChildJobId().split(",");
			for (String childJobIdItem: childJobIds) {
				if (childJobIdItem!=null && childJobIdItem.trim().length()>0 && isNumeric(childJobIdItem)) {
					XxlJobInfo childJobInfo = xxlJobInfoDao.loadById(Long.parseLong(childJobIdItem));
					if (childJobInfo==null) {
						return new ReturnT<String>(ReturnT.FAIL_CODE,
								MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId")+"({0})"+I18nUtil.getString("system_not_found")), childJobIdItem));
					}
					if (!loginUser.validPermission(childJobInfo.getJobGroup())) {
						return new ReturnT<String>(ReturnT.FAIL_CODE,
								MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId")+"({0})"+I18nUtil.getString("system_permission_limit")), childJobIdItem));
					}
				} else {
					return new ReturnT<String>(ReturnT.FAIL_CODE,
							MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId")+"({0})"+I18nUtil.getString("system_unvalid")), childJobIdItem));
				}
			}

			// join , avoid "xxx,,"
			String temp = "";
			for (String item:childJobIds) {
				temp += item + ",";
			}
			temp = temp.substring(0, temp.length()-1);

			jobInfo.setChildJobId(temp);
		}

		// add in db
		jobInfo.setAddTime(new Date());
		jobInfo.setUpdateTime(new Date());
		jobInfo.setGlueUpdatetime(new Date());
		xxlJobInfoDao.save(jobInfo);
		if (jobInfo.getId() < 1) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_add")+I18nUtil.getString("system_fail")) );
		}

		return new ReturnT<String>(String.valueOf(jobInfo.getId()));
	}

	private boolean isNumeric(String str){
		try {
			int result = Integer.valueOf(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	@Override
	public ReturnT<String> update(XxlJobInfo jobInfo, XxlJobUser loginUser) {

		// valid base
		if (jobInfo.getJobDesc()==null || jobInfo.getJobDesc().trim().length()==0) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input")+I18nUtil.getString("jobinfo_field_jobdesc")) );
		}
		if (jobInfo.getAuthor()==null || jobInfo.getAuthor().trim().length()==0) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input")+I18nUtil.getString("jobinfo_field_author")) );
		}

		// valid trigger
		ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
		if (scheduleTypeEnum == null) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")+I18nUtil.getString("system_unvalid")) );
		}
		if (scheduleTypeEnum == ScheduleTypeEnum.CRON) {
			if (jobInfo.getScheduleConf()==null || !CronExpression.isValidExpression(jobInfo.getScheduleConf())) {
				return new ReturnT<String>(ReturnT.FAIL_CODE, "Cron"+I18nUtil.getString("system_unvalid") );
			}
		} else if (scheduleTypeEnum == ScheduleTypeEnum.FIX_RATE /*|| scheduleTypeEnum == ScheduleTypeEnum.FIX_DELAY*/) {
			if (jobInfo.getScheduleConf() == null) {
				return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")+I18nUtil.getString("system_unvalid")) );
			}
			try {
				int fixSecond = Integer.valueOf(jobInfo.getScheduleConf());
				if (fixSecond < 1) {
					return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")+I18nUtil.getString("system_unvalid")) );
				}
			} catch (Exception e) {
				return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")+I18nUtil.getString("system_unvalid")) );
			}
		}

		// valid advanced
		if (ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null) == null) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorRouteStrategy")+I18nUtil.getString("system_unvalid")) );
		}
		if (MisfireStrategyEnum.match(jobInfo.getMisfireStrategy(), null) == null) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("misfire_strategy")+I18nUtil.getString("system_unvalid")) );
		}
		if (ExecutorBlockStrategyEnum.match(jobInfo.getExecutorBlockStrategy(), null) == null) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorBlockStrategy")+I18nUtil.getString("system_unvalid")) );
		}

		// 》ChildJobId valid
		if (jobInfo.getChildJobId()!=null && jobInfo.getChildJobId().trim().length()>0) {
			String[] childJobIds = jobInfo.getChildJobId().split(",");
			for (String childJobIdItem: childJobIds) {
				if (childJobIdItem!=null && childJobIdItem.trim().length()>0 && isNumeric(childJobIdItem)) {
					// parse child
					Long childJobId = Long.parseLong(childJobIdItem);
					if (childJobId == jobInfo.getId()) {
						return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_childJobId")+"("+childJobId+")"+I18nUtil.getString("system_unvalid")) );
					}

					// valid child
					XxlJobInfo childJobInfo = xxlJobInfoDao.loadById(childJobId);
					if (childJobInfo==null) {
						return new ReturnT<String>(ReturnT.FAIL_CODE,
								MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId")+"({0})"+I18nUtil.getString("system_not_found")), childJobIdItem));
					}
					if (!loginUser.validPermission(childJobInfo.getJobGroup())) {
						return new ReturnT<String>(ReturnT.FAIL_CODE,
								MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId")+"({0})"+I18nUtil.getString("system_permission_limit")), childJobIdItem));
					}
				} else {
					return new ReturnT<String>(ReturnT.FAIL_CODE,
							MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId")+"({0})"+I18nUtil.getString("system_unvalid")), childJobIdItem));
				}
			}

			// join , avoid "xxx,,"
			String temp = "";
			for (String item:childJobIds) {
				temp += item + ",";
			}
			temp = temp.substring(0, temp.length()-1);

			jobInfo.setChildJobId(temp);
		}

		// group valid
		XxlJobGroup jobGroup = xxlJobGroupDao.load(jobInfo.getJobGroup());
		if (jobGroup == null) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_jobgroup")+I18nUtil.getString("system_unvalid")) );
		}

		// stage job info
		XxlJobInfo exists_jobInfo = xxlJobInfoDao.loadById(jobInfo.getId());
		if (exists_jobInfo == null) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_id")+I18nUtil.getString("system_not_found")) );
		}

		// next trigger time (5s后生效，避开预读周期)
		long nextTriggerTime = exists_jobInfo.getTriggerNextTime();
		boolean scheduleDataNotChanged = jobInfo.getScheduleType().equals(exists_jobInfo.getScheduleType()) && jobInfo.getScheduleConf().equals(exists_jobInfo.getScheduleConf());
		if (exists_jobInfo.getTriggerStatus() == 1 && !scheduleDataNotChanged) {
			try {
				Date nextValidTime = JobScheduleHelper.generateNextValidTime(jobInfo, new Date(System.currentTimeMillis() + JobScheduleHelper.PRE_READ_MS));
				if (nextValidTime == null) {
					return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")+I18nUtil.getString("system_unvalid")) );
				}
				nextTriggerTime = nextValidTime.getTime();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")+I18nUtil.getString("system_unvalid")) );
			}
		}

		exists_jobInfo.setJobGroup(jobInfo.getJobGroup());
		exists_jobInfo.setJobDesc(jobInfo.getJobDesc());
		exists_jobInfo.setAuthor(jobInfo.getAuthor());
		exists_jobInfo.setAlarmEmail(jobInfo.getAlarmEmail());
		exists_jobInfo.setScheduleType(jobInfo.getScheduleType());
		exists_jobInfo.setScheduleConf(jobInfo.getScheduleConf());
		exists_jobInfo.setMisfireStrategy(jobInfo.getMisfireStrategy());
		exists_jobInfo.setExecutorRouteStrategy(jobInfo.getExecutorRouteStrategy());
		exists_jobInfo.setExecutorHandler(jobInfo.getExecutorHandler());
		exists_jobInfo.setExecutorParam(jobInfo.getExecutorParam());
		exists_jobInfo.setExecutorBlockStrategy(jobInfo.getExecutorBlockStrategy());
		exists_jobInfo.setExecutorTimeout(jobInfo.getExecutorTimeout());
		exists_jobInfo.setExecutorFailRetryCount(jobInfo.getExecutorFailRetryCount());
		exists_jobInfo.setChildJobId(jobInfo.getChildJobId());
		exists_jobInfo.setTriggerNextTime(nextTriggerTime);

		exists_jobInfo.setUpdateTime(new Date());
        xxlJobInfoDao.update(exists_jobInfo);


		return ReturnT.SUCCESS;
	}

	@Override
	public ReturnT<String> remove(Long id) {
		XxlJobInfo xxlJobInfo = xxlJobInfoDao.loadById(id);
		if (xxlJobInfo == null) {
			return ReturnT.SUCCESS;
		}

		xxlJobInfoDao.delete(id);
		xxlJobLogDao.delete(id);
		xxlJobLogGlueDao.deleteByJobId(id);
		return ReturnT.SUCCESS;
	}

	@Override
	public ReturnT<String> start(Long id) {
		// load and valid
		XxlJobInfo xxlJobInfo = xxlJobInfoDao.loadById(id);
		if (xxlJobInfo == null) {
			return new ReturnT<String>(ReturnT.FAIL.getCode(), I18nUtil.getString("jobinfo_glue_jobid_unvalid"));
		}

		// valid
		ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(xxlJobInfo.getScheduleType(), ScheduleTypeEnum.NONE);
		if (ScheduleTypeEnum.NONE == scheduleTypeEnum) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type_none_limit_start")) );
		}

		// next trigger time (5s后生效，避开预读周期)
		long nextTriggerTime = 0;
		try {
			Date nextValidTime = JobScheduleHelper.generateNextValidTime(xxlJobInfo, new Date(System.currentTimeMillis() + JobScheduleHelper.PRE_READ_MS));
			if (nextValidTime == null) {
				return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")+I18nUtil.getString("system_unvalid")) );
			}
			nextTriggerTime = nextValidTime.getTime();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")+I18nUtil.getString("system_unvalid")) );
		}

		xxlJobInfo.setTriggerStatus(1);
		xxlJobInfo.setTriggerLastTime(0);
		xxlJobInfo.setTriggerNextTime(nextTriggerTime);

		xxlJobInfo.setUpdateTime(new Date());
		xxlJobInfoDao.update(xxlJobInfo);
		return ReturnT.SUCCESS;
	}

	@Override
	public ReturnT<String> stop(Long id) {
		// load and valid
        XxlJobInfo xxlJobInfo = xxlJobInfoDao.loadById(id);
		if (xxlJobInfo == null) {
			return new ReturnT<String>(ReturnT.FAIL.getCode(), I18nUtil.getString("jobinfo_glue_jobid_unvalid"));
		}

		// stop
		xxlJobInfo.setTriggerStatus(0);
		xxlJobInfo.setTriggerLastTime(0);
		xxlJobInfo.setTriggerNextTime(0);

		xxlJobInfo.setUpdateTime(new Date());
		xxlJobInfoDao.update(xxlJobInfo);
		return ReturnT.SUCCESS;
	}

	/**
	 * 手动触发任务的方法。
	 *
	 * @param loginUser     当前登录的用户信息，用于权限验证。
	 * @param jobId         任务的唯一标识符，用于指定要触发的任务。
	 * @param executorParam 执行参数，传递给任务执行器。
	 * @param addressList   执行器地址列表，用于覆盖默认的执行器地址。
	 * @return              返回任务触发的结果，包含成功或失败的信息。
	 */
	@Override
	public ReturnT<String> trigger(XxlJobUser loginUser, Long jobId, String executorParam, String addressList) {
		// 权限验证，确保用户有权限触发该任务
		if (loginUser == null) {
			return new ReturnT<String>(ReturnT.FAIL.getCode(), I18nUtil.getString("system_permission_limit"));
		}
		XxlJobInfo xxlJobInfo = xxlJobInfoDao.loadById(jobId);
		if (xxlJobInfo == null) {
			return new ReturnT<String>(ReturnT.FAIL.getCode(), I18nUtil.getString("jobinfo_glue_jobid_unvalid"));
		}
		if (!hasPermission(loginUser, xxlJobInfo.getJobGroup())) {
			return new ReturnT<String>(ReturnT.FAIL.getCode(), I18nUtil.getString("system_permission_limit"));
		}

		// 强制覆盖任务参数
		if (executorParam == null) {
			executorParam = "";
		}

		// 使用JobTriggerPoolHelper触发任务
		JobTriggerPoolHelper.trigger(jobId, TriggerTypeEnum.MANUAL, -1, null, executorParam, addressList);
		return ReturnT.SUCCESS;
	}

	@Override
	@Transactional
	public ReturnT<String> ShardingTrigger(HandleShardingParam handleShardingParam) {
		// 1. 参数校验
		validateParams(handleShardingParam);

		// 2. 获取执行器信息
		XxlJobGroup xxlJobGroup = getXxlJobGroup(handleShardingParam.getAppName());
		if (xxlJobGroup == null) {
			logger.error("执行器 {} 不存在！", handleShardingParam.getAppName());
			throw new IllegalArgumentException("执行器 " + handleShardingParam.getAppName() + " 不存在！");
		}

		// 3. 解析时间范围
		List<TimeRange> timeRanges = TimeRangeSplitterUtils.splitTimeRange(
				handleShardingParam.getStartTimeOfData(),
				handleShardingParam.getEndTimeOfData(),
				handleShardingParam.getDataInterval(),
				handleShardingParam.getTimeUnit().name()
		);

		// 4. 构建父任务信息
		XxlJobInfo xxlJobInfo = buildXxlJobInfo(handleShardingParam, xxlJobGroup);

		// 5. 构建子任务信息
		List<XxlJobShardingInfo> xxlJobShardingInfos = buildXxlJobShardingInfos(
				handleShardingParam,
				timeRanges,
				xxlJobInfo.getId()
		);

		// 6. 保存任务信息
		saveTaskInfo(xxlJobInfo, xxlJobShardingInfos);

		// 7. 触发任务
		triggerTask(xxlJobShardingInfos, handleShardingParam.getIsAutomatic());

		return ReturnT.SUCCESS;
	}

	// 参数校验
	private void validateParams(HandleShardingParam handleShardingParam) {
		if (handleShardingParam == null) {
			throw new IllegalArgumentException("任务参数不能为空！");
		}
		// 其他参数校验逻辑
		// 校验任务ID
		if (handleShardingParam.getId() == null) {
			throw new IllegalArgumentException("任务ID不能为空");
		}

		// 校验执行器服务名称
		if (handleShardingParam.getAppName() == null || handleShardingParam.getAppName().trim().isEmpty()) {
			throw new IllegalArgumentException("执行器服务名称不能为空");
		}

		// 校验执行方法
		if (handleShardingParam.getExecuteHandle() == null || handleShardingParam.getExecuteHandle().trim().isEmpty()) {
			throw new IllegalArgumentException("执行方法不能为空");
		}

		// 校验是否手动触发
		if (ObjectUtils.isEmpty(handleShardingParam.getIsAutomatic())||(handleShardingParam.getIsAutomatic() != 0 && handleShardingParam.getIsAutomatic() != 1)) {
			throw new IllegalArgumentException("是否手动触发值必须为0或1");
		}

		// 校验优先级
		if (handleShardingParam.getIsAutomatic() == 1 && (handleShardingParam.getPriority() < 0||handleShardingParam.getPriority()>10)) {
			throw new IllegalArgumentException("优先级必须大于或等于0,小于10");
		}

		// 校验调度周期
		if (handleShardingParam.getSchedulingCycle() == null) {
			throw new IllegalArgumentException("调度周期不能为空");
		}

		// 校验调度间隔
		if (ObjectUtils.isEmpty(handleShardingParam.getSchedulingInterval())||handleShardingParam.getSchedulingInterval() <= 0) {
			throw new IllegalArgumentException("调度间隔必须大于0");
		}

		// 校验时间格式
		String timePattern = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$";
		if (handleShardingParam.getFirstSchedulingTime() != null && !Pattern.matches(timePattern, handleShardingParam.getFirstSchedulingTime())) {
			throw new IllegalArgumentException("首次调度时间格式必须为yyyy-MM-dd HH:mm:ss");
		}
		if (handleShardingParam.getSchedulingDeadline() != null && !Pattern.matches(timePattern, handleShardingParam.getSchedulingDeadline())) {
			throw new IllegalArgumentException("调度截止时间格式必须为yyyy-MM-dd HH:mm:ss");
		}
		if (handleShardingParam.getStartTimeOfData() != null && !Pattern.matches(timePattern, handleShardingParam.getStartTimeOfData())) {
			throw new IllegalArgumentException("数据开始时间格式必须为yyyy-MM-dd HH:mm:ss");
		}
		if (handleShardingParam.getEndTimeOfData() != null && !Pattern.matches(timePattern, handleShardingParam.getEndTimeOfData())) {
			throw new IllegalArgumentException("数据截止时间格式必须为yyyy-MM-dd HH:mm:ss");
		}

		// 校验数据时间间隔
		if (ObjectUtils.isEmpty(handleShardingParam.getDataInterval())&&handleShardingParam.getDataInterval() <= 0) {
			throw new IllegalArgumentException("数据时间间隔必须大于0");
		}

		// 校验数据时间间隔单位
		if (handleShardingParam.getTimeUnit() == null) {
			throw new IllegalArgumentException("数据时间间隔单位不能为空");
		}

		// 校验时间顺序
		if (handleShardingParam.getFirstSchedulingTime() != null && handleShardingParam.getSchedulingDeadline() != null) {
			if (handleShardingParam.getFirstSchedulingTime().compareTo(handleShardingParam.getSchedulingDeadline()) > 0) {
				throw new IllegalArgumentException("首次调度时间不能晚于调度截止时间");
			}
		}

		if (handleShardingParam.getStartTimeOfData() != null && handleShardingParam.getEndTimeOfData() != null) {
			if (handleShardingParam.getStartTimeOfData().compareTo(handleShardingParam.getEndTimeOfData()) > 0) {
				throw new IllegalArgumentException("数据开始时间不能晚于数据截止时间");
			}
		}
	}

	// 获取执行器信息
	private XxlJobGroup getXxlJobGroup(String appName) {
		return XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().loadByAppName(appName);
	}

	// 构建父任务信息
	private XxlJobInfo buildXxlJobInfo(HandleShardingParam handleShardingParam, XxlJobGroup xxlJobGroup) {
		XxlJobInfo xxlJobInfo = new XxlJobInfo();
		xxlJobInfo.setId(handleShardingParam.getId());
		xxlJobInfo.setJobGroup(xxlJobGroup.getId());
		xxlJobInfo.setAddTime(new Date());
		xxlJobInfo.setUpdateTime(new Date());
		xxlJobInfo.setJobDesc("同步任务");
		xxlJobInfo.setPriority(handleShardingParam.getPriority());
		xxlJobInfo.setScheduleType(ScheduleTypeEnum.PERIOD.name());
		xxlJobInfo.setMisfireStrategy(MisfireStrategyEnum.DO_NOTHING.name());
		xxlJobInfo.setExecutorRouteStrategy(ExecutorRouteStrategyEnum.FIRST.name());
		xxlJobInfo.setExecutorHandler(handleShardingParam.getExecuteHandle());
		xxlJobInfo.setExecutorParam(JSONUtil.toJsonStr(new TimeRange(
				handleShardingParam.getStartTimeOfData(),
				handleShardingParam.getEndTimeOfData()
		)));
		xxlJobInfo.setExecutorBlockStrategy(ExecutorBlockStrategyEnum.SERIAL_EXECUTION.name());
		xxlJobInfo.setGlueType(GlueTypeEnum.BEAN.getDesc());
		xxlJobInfo.setGlueUpdatetime(new Date());
		xxlJobInfo.setFirstSchedulingTime(handleShardingParam.getFirstSchedulingTime());
		xxlJobInfo.setSchedulingDeadline(handleShardingParam.getSchedulingDeadline());
		xxlJobInfo.setTriggerNextTime(TimeConverterUtil.convertToTimestamp(handleShardingParam.getFirstSchedulingTime()));
		xxlJobInfo.setTimeUnit(handleShardingParam.getTimeUnit().name());
		xxlJobInfo.setDataInterval(handleShardingParam.getDataInterval());
		xxlJobInfo.setSchedulingInterval(handleShardingParam.getSchedulingInterval());
		xxlJobInfo.setSchedulingCycle(handleShardingParam.getSchedulingCycle().name());
		xxlJobInfo.setTriggerStatus(handleShardingParam.getIsAutomatic() == 1 ? 1 : 0);
		xxlJobInfo.setPriority(handleShardingParam.getPriority());
		xxlJobInfo.setIsAutomatic(handleShardingParam.getIsAutomatic() == 1 ? 1 : 0);
		return xxlJobInfo;
	}

	// 构建子任务信息
	private List<XxlJobShardingInfo> buildXxlJobShardingInfos(
			HandleShardingParam handleShardingParam,
			List<TimeRange> timeRanges,
			Long parentJobId
	) {
		int maxExecuteBatch = XxlJobAdminConfig.getAdminConfig().getXxlJobShardingInfoDao().findMaxExecuteBatch(parentJobId);
		List<XxlJobShardingInfo> xxlJobShardingInfos = new ArrayList<>();

		for (TimeRange timeRange : timeRanges) {
			XxlJobShardingInfo xxlJobShardingInfo = new XxlJobShardingInfo();
			xxlJobShardingInfo.setId(IdUtil.getSnowflakeNextId());
			xxlJobShardingInfo.setParams(JSONUtil.toJsonStr(timeRange));
			xxlJobShardingInfo.setParentJobId(parentJobId);
			xxlJobShardingInfo.setTriggerTime(handleShardingParam.getIsAutomatic() == 1
					? DateUtil.parseDateTime(handleShardingParam.getFirstSchedulingTime())
					: new Date());
			xxlJobShardingInfo.setExecuteBatch(maxExecuteBatch + 1);
			xxlJobShardingInfo.setExecuteState(ExecutionStatus.NOT_EXECUTED.getCode());
			xxlJobShardingInfos.add(xxlJobShardingInfo);
		}
		return xxlJobShardingInfos;
	}

	// 保存任务信息
	private void saveTaskInfo(XxlJobInfo xxlJobInfo, List<XxlJobShardingInfo> xxlJobShardingInfos) {
		XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().delete(xxlJobInfo.getId());
		XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().save(xxlJobInfo);

		XxlJobAdminConfig.getAdminConfig().getXxlJobShardingInfoDao().logicDeleteByParentId(xxlJobInfo.getId());
		XxlJobAdminConfig.getAdminConfig().getXxlJobShardingInfoDao().bathSave(xxlJobShardingInfos);
	}

	// 触发任务
	private void triggerTask(List<XxlJobShardingInfo> xxlJobShardingInfos, Integer isAutomatic) {
		if (isAutomatic == null || isAutomatic != 1) {
			// 把子任务放队列
			for (XxlJobShardingInfo xxlJobShardingInfo : xxlJobShardingInfos) {
				sortedTaskService.addTask(new SortedTask(xxlJobShardingInfo.getId().toString(),xxlJobShardingInfo.getJobDesc(),xxlJobShardingInfo.getJobDesc(),0));
			}
		}
	}


	private boolean hasPermission(XxlJobUser loginUser, int jobGroup){
		if (loginUser.getRole() == 1) {
			return true;
		}
		List<String> groupIdStrs = new ArrayList<>();
		if (loginUser.getPermission()!=null && loginUser.getPermission().trim().length()>0) {
			groupIdStrs = Arrays.asList(loginUser.getPermission().trim().split(","));
		}
		return groupIdStrs.contains(String.valueOf(jobGroup));
	}

	@Override
	public Map<String, Object> dashboardInfo() {

		int jobInfoCount = xxlJobInfoDao.findAllCount();
		int jobLogCount = 0;
		int jobLogSuccessCount = 0;
		XxlJobLogReport xxlJobLogReport = xxlJobLogReportDao.queryLogReportTotal();
		if (xxlJobLogReport != null) {
			jobLogCount = xxlJobLogReport.getRunningCount() + xxlJobLogReport.getSucCount() + xxlJobLogReport.getFailCount();
			jobLogSuccessCount = xxlJobLogReport.getSucCount();
		}

		// executor count
		Set<String> executorAddressSet = new HashSet<String>();
		List<XxlJobGroup> groupList = xxlJobGroupDao.findAll();

		if (groupList!=null && !groupList.isEmpty()) {
			for (XxlJobGroup group: groupList) {
				if (group.getRegistryList()!=null && !group.getRegistryList().isEmpty()) {
					executorAddressSet.addAll(group.getRegistryList());
				}
			}
		}

		int executorCount = executorAddressSet.size();

		Map<String, Object> dashboardMap = new HashMap<String, Object>();
		dashboardMap.put("jobInfoCount", jobInfoCount);
		dashboardMap.put("jobLogCount", jobLogCount);
		dashboardMap.put("jobLogSuccessCount", jobLogSuccessCount);
		dashboardMap.put("executorCount", executorCount);
		return dashboardMap;
	}

	@Override
	public ReturnT<Map<String, Object>> chartInfo(Date startDate, Date endDate) {

		// process
		List<String> triggerDayList = new ArrayList<String>();
		List<Integer> triggerDayCountRunningList = new ArrayList<Integer>();
		List<Integer> triggerDayCountSucList = new ArrayList<Integer>();
		List<Integer> triggerDayCountFailList = new ArrayList<Integer>();
		int triggerCountRunningTotal = 0;
		int triggerCountSucTotal = 0;
		int triggerCountFailTotal = 0;

		List<XxlJobLogReport> logReportList = xxlJobLogReportDao.queryLogReport(startDate, endDate);

		if (logReportList!=null && logReportList.size()>0) {
			for (XxlJobLogReport item: logReportList) {
				String day = DateUtil.formatDate(item.getTriggerDay());
				int triggerDayCountRunning = item.getRunningCount();
				int triggerDayCountSuc = item.getSucCount();
				int triggerDayCountFail = item.getFailCount();

				triggerDayList.add(day);
				triggerDayCountRunningList.add(triggerDayCountRunning);
				triggerDayCountSucList.add(triggerDayCountSuc);
				triggerDayCountFailList.add(triggerDayCountFail);

				triggerCountRunningTotal += triggerDayCountRunning;
				triggerCountSucTotal += triggerDayCountSuc;
				triggerCountFailTotal += triggerDayCountFail;
			}
		} else {
			for (int i = -6; i <= 0; i++) {
				triggerDayList.add(DateUtil.formatDate(DateUtil.addDays(new Date(), i)));
				triggerDayCountRunningList.add(0);
				triggerDayCountSucList.add(0);
				triggerDayCountFailList.add(0);
			}
		}

		Map<String, Object> result = new HashMap<String, Object>();
		result.put("triggerDayList", triggerDayList);
		result.put("triggerDayCountRunningList", triggerDayCountRunningList);
		result.put("triggerDayCountSucList", triggerDayCountSucList);
		result.put("triggerDayCountFailList", triggerDayCountFailList);

		result.put("triggerCountRunningTotal", triggerCountRunningTotal);
		result.put("triggerCountSucTotal", triggerCountSucTotal);
		result.put("triggerCountFailTotal", triggerCountFailTotal);

		return new ReturnT<Map<String, Object>>(result);
	}

}
