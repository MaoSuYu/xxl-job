#
# XXL-JOB
# Copyright (c) 2015-present, xuxueli.

CREATE database if NOT EXISTS `xxl_job` default character set utf8mb4 collate utf8mb4_unicode_ci;
use
`xxl_job`;

SET NAMES utf8mb4;
CREATE TABLE `xxl_job_sharding_info` (
                                         `id` bigint(20) NOT NULL COMMENT '分片子任务id',
                                         `parent_job_id` bigint(20) NOT NULL COMMENT '父任务id',
                                         `params` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '参数（子任务的时间参数）',
                                         `trigger_time` datetime DEFAULT NULL COMMENT '更新时间',
                                         `execute_batch` int(11) DEFAULT NULL COMMENT '执行批次',
                                         `delete_flag` int(2) DEFAULT '0' COMMENT '删除标记',
                                         `execute_state` int(2) DEFAULT '0' COMMENT '执行状态0未执行1执行成功2执行失败',
                                         `execute_number` int(11) DEFAULT '0' COMMENT '执行次数',
                                         `is_automatic` int(2) DEFAULT NULL COMMENT '是否自动0手动1自动',
                                         PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `xxl_job_info`
(
    `id` bigint(20) NOT NULL AUTO_INCREMENT,
    `job_group` int(11) NOT NULL COMMENT '执行器主键ID',
    `job_desc` varchar(255) NOT NULL,
    `add_time` datetime DEFAULT NULL,
    `update_time` datetime DEFAULT NULL,
    `author` varchar(64) DEFAULT NULL COMMENT '作者',
    `alarm_email` varchar(255) DEFAULT NULL COMMENT '报警邮件',
    `schedule_type` varchar(50) DEFAULT 'NONE' COMMENT '调度类型',
    `schedule_conf` varchar(128) DEFAULT NULL COMMENT '调度配置，值含义取决于调度类型',
    `misfire_strategy` varchar(50) DEFAULT 'DO_NOTHING' COMMENT '调度过期策略',
    `executor_route_strategy` varchar(50) DEFAULT NULL COMMENT '执行器路由策略',
    `executor_handler` varchar(255) DEFAULT NULL COMMENT '执行器任务handler',
    `executor_param` varchar(512) DEFAULT NULL COMMENT '执行器任务参数',
    `executor_block_strategy` varchar(50) DEFAULT NULL COMMENT '阻塞处理策略',
    `executor_timeout` int(11) DEFAULT '0' COMMENT '任务执行超时时间，单位秒',
    `executor_fail_retry_count` int(11) DEFAULT '0' COMMENT '失败重试次数',
    `glue_type` varchar(50) DEFAULT NULL COMMENT 'GLUE类型',
    `glue_source` mediumtext COMMENT 'GLUE源代码',
    `glue_remark` varchar(128) DEFAULT NULL COMMENT 'GLUE备注',
    `glue_updatetime` datetime DEFAULT NULL COMMENT 'GLUE更新时间',
    `child_jobid` varchar(255) DEFAULT NULL COMMENT '子任务ID，多个逗号分隔',
    `trigger_status` tinyint(4) DEFAULT '0' COMMENT '调度状态：0-停止，1-运行',
    `trigger_last_time` bigint(13) DEFAULT '0' COMMENT '上次调度时间',
    `trigger_next_time` bigint(13) DEFAULT '0' COMMENT '下次调度时间',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

ALTER TABLE `xxl_job_info`
    ADD COLUMN `first_scheduling_time` datetime DEFAULT NULL COMMENT '首次调度时间',
    ADD COLUMN `start_time_of_data` datetime DEFAULT NULL COMMENT '数据开始时间',
    ADD COLUMN `end_time_of_data` datetime DEFAULT NULL COMMENT '数据截止时间',
    ADD COLUMN `scheduling_dead_line` datetime DEFAULT NULL COMMENT '截止调度时间',
    ADD COLUMN `data_interval` int(11) DEFAULT NULL COMMENT '数据间隔',
    ADD COLUMN `time_unit` varchar(50) DEFAULT NULL COMMENT '数据时间单位MINUTE, HOUR, DAY, WEEK, MONTH',
    ADD COLUMN `scheduling_interval` int(11) DEFAULT NULL COMMENT '调度间隔',
    ADD COLUMN `scheduling_cycle` varchar(50) DEFAULT NULL COMMENT '调度时间单位MINUTE, HOUR, DAY, WEEK, MONTH',
    ADD COLUMN `is_automatic` int(2) DEFAULT NULL COMMENT '是否自动，0手动1自动',
    ADD COLUMN `priority` int(2) DEFAULT NULL COMMENT '优先级越低优先级越高',
    ADD COLUMN `remote_id` bigint(11) NOT NULL COMMENT '远程任务id';

CREATE TABLE `xxl_job_log`
(
    `id`                        bigint(20) NOT NULL AUTO_INCREMENT,
    `job_group`                 int(11)    NOT NULL COMMENT '执行器主键ID',
    `job_id`                    int(11)    NOT NULL COMMENT '任务，主键ID',
    `executor_address`          varchar(255) DEFAULT NULL COMMENT '执行器地址，本次执行的地址',
    `executor_handler`          varchar(255) DEFAULT NULL COMMENT '执行器任务handler',
    `executor_param`            varchar(512) DEFAULT NULL COMMENT '执行器任务参数',
    `executor_sharding_param`   varchar(20)  DEFAULT NULL COMMENT '执行器任务分片参数，格式如 1/2',
    `executor_fail_retry_count` int(11)    NOT NULL DEFAULT '0' COMMENT '失败重试次数',
    `trigger_time`              datetime     DEFAULT NULL COMMENT '调度-时间',
    `trigger_code`              int(11)    NOT NULL COMMENT '调度-结果',
    `trigger_msg`               text COMMENT '调度-日志',
    `handle_time`               datetime     DEFAULT NULL COMMENT '执行-时间',
    `handle_code`               int(11)    NOT NULL COMMENT '执行-状态',
    `handle_msg`                text COMMENT '执行-日志',
    `alarm_status`              tinyint(4) NOT NULL DEFAULT '0' COMMENT '告警状态：0-默认、1-无需告警、2-告警成功、3-告警失败',
    PRIMARY KEY (`id`),
    KEY                         `I_trigger_time` (`trigger_time`),
    KEY                         `I_handle_code` (`handle_code`),
    KEY                         `I_jobid_jobgroup` (`job_id`,`job_group`),
    KEY                         `I_job_id` (`job_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

CREATE TABLE `xxl_job_log_report`
(
    `id`            int(11) NOT NULL AUTO_INCREMENT,
    `trigger_day`   datetime DEFAULT NULL COMMENT '调度-时间',
    `running_count` int(11) NOT NULL DEFAULT '0' COMMENT '运行中-日志数量',
    `suc_count`     int(11) NOT NULL DEFAULT '0' COMMENT '执行成功-日志数量',
    `fail_count`    int(11) NOT NULL DEFAULT '0' COMMENT '执行失败-日志数量',
    `update_time`   datetime DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `i_trigger_day` (`trigger_day`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

CREATE TABLE `xxl_job_logglue`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT,
    `job_id`      int(11)      NOT NULL COMMENT '任务，主键ID',
    `glue_type`   varchar(50) DEFAULT NULL COMMENT 'GLUE类型',
    `glue_source` mediumtext COMMENT 'GLUE源代码',
    `glue_remark` varchar(128) NOT NULL COMMENT 'GLUE备注',
    `add_time`    datetime    DEFAULT NULL,
    `update_time` datetime    DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;


CREATE TABLE `xxl_job_registry`
(
    `id`                   int(11)      NOT NULL AUTO_INCREMENT,
    `registry_group`       varchar(50)  NOT NULL,
    `registry_key`         varchar(255) NOT NULL,
    `registry_value`       varchar(255) NOT NULL,
    `update_time`          datetime              DEFAULT NULL,
    `thread_running_count` int          NOT NULL DEFAULT 0,
    `max_thread_count`     int          NOT NULL DEFAULT 10,
    PRIMARY KEY (`id`),
    UNIQUE KEY `i_g_k_v` (`registry_group`, `registry_key`, `registry_value`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;


CREATE TABLE `xxl_job_group`
(
    `id`           int(11)     NOT NULL AUTO_INCREMENT,
    `app_name`     varchar(64) NOT NULL COMMENT '执行器AppName',
    `title`        varchar(12) NOT NULL COMMENT '执行器名称',
    `address_type` tinyint(4)  NOT NULL DEFAULT '0' COMMENT '执行器地址类型：0=自动注册、1=手动录入',
    `address_list` text COMMENT '执行器地址列表，多地址逗号分隔',
    `update_time`  datetime DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

CREATE TABLE `xxl_job_user`
(
    `id`         int(11)     NOT NULL AUTO_INCREMENT,
    `username`   varchar(50) NOT NULL COMMENT '账号',
    `password`   varchar(50) NOT NULL COMMENT '密码',
    `role`       tinyint(4)  NOT NULL COMMENT '角色：0-普通用户、1-管理员',
    `permission` varchar(255) DEFAULT NULL COMMENT '权限：执行器ID列表，多个逗号分割',
    PRIMARY KEY (`id`),
    UNIQUE KEY `i_username` (`username`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

CREATE TABLE `xxl_job_lock`
(
    `lock_name` varchar(50) NOT NULL COMMENT '锁名称',
    PRIMARY KEY (`lock_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;


CREATE TABLE `xxl_job_task_executor_mapping`
(
    `id`               bigint       NOT NULL AUTO_INCREMENT COMMENT 'ID',
    `job_id`           int          NOT NULL COMMENT '任务ID',
    `executor_address` varchar(255) NOT NULL COMMENT '执行器地址',
    `update_time`      datetime     NOT NULL COMMENT '更新时间',
    `group_id`         bigint       NOT NULL COMMENT '执行器组id',
    `app_name`         varchar(64)  NOT NULL COMMENT '执行器AppName',
    `title`            varchar(12)  NOT NULL COMMENT '执行器名称',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uidx_job_id` (`job_id`),
    KEY                `idx_update_time` (`update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务执行器节点映射表';


INSERT INTO `xxl_job_group`(`id`, `app_name`, `title`, `address_type`, `address_list`, `update_time`)
VALUES (1, 'xxl-job-executor-sample', '示例执行器', 0, NULL, '2018-11-03 22:21:31');

INSERT INTO `xxl_job_info`(`id`, `job_group`, `job_desc`, `add_time`, `update_time`, `author`, `alarm_email`,
                           `schedule_type`, `schedule_conf`, `misfire_strategy`, `executor_route_strategy`,
                           `executor_handler`, `executor_param`, `executor_block_strategy`, `executor_timeout`,
                           `executor_fail_retry_count`, `glue_type`, `glue_source`, `glue_remark`, `glue_updatetime`,
                           `child_jobid`)
VALUES (1, 1, '测试任务1', '2018-11-03 22:21:31', '2018-11-03 22:21:31', 'XXL', '', 'CRON', '0 0 0 * * ? *',
        'DO_NOTHING', 'FIRST', 'demoJobHandler', '', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化',
        '2018-11-03 22:21:31', '');

INSERT INTO `xxl_job_user`(`id`, `username`, `password`, `role`, `permission`)
VALUES (1, 'admin', 'e10adc3949ba59abbe56e057f20f883e', 1, NULL);

INSERT INTO `xxl_job_lock` (`lock_name`)
VALUES ('schedule_lock');

commit;

