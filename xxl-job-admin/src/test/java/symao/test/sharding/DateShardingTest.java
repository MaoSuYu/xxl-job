package symao.test.sharding;

import com.xxl.job.core.util.DateShardingUtil;
import com.xxl.job.core.util.DateShardingUtil.TimeUnit;
import com.xxl.job.core.util.DateShardingUtil.TimeShard;
import java.util.List;

/**
 * 日期分片工具测试类
 * 
 * @author symao
 */
public class DateShardingTest {

    public static void main(String[] args) {
        // 示例：将2025-03-11 19:38到2025-03-11 23:38的时间范围按5分钟分片
        String startTime = "2025-03-11 19:38";
        String endTime = "2025-03-11 23:38";
        
        List<TimeShard> shards = DateShardingUtil.shardTimeRange(startTime, endTime, TimeUnit.MINUTE, 5);
        
        System.out.println("将时间范围 " + startTime + " 到 " + endTime + " 按5分钟进行分片：");
        for (int i = 0; i < shards.size(); i++) {
            System.out.println("分片 " + (i + 1) + ": " + shards.get(i));
        }
        
        // 小时分片示例
        System.out.println("\n将时间范围 " + startTime + " 到 " + endTime + " 按1小时进行分片：");
        shards = DateShardingUtil.shardTimeRange(startTime, endTime, TimeUnit.HOUR, 1);
        for (int i = 0; i < shards.size(); i++) {
            System.out.println("分片 " + (i + 1) + ": " + shards.get(i));
        }
        
        // 天分片示例
        System.out.println("\n将时间范围 " + startTime + " 到 " + endTime + " 按1天进行分片：");
        shards = DateShardingUtil.shardTimeRange(startTime, endTime, TimeUnit.DAY, 1);
        for (int i = 0; i < shards.size(); i++) {
            System.out.println("分片 " + (i + 1) + ": " + shards.get(i));
        }
        
        // 测试不满足完整分片时间的情况
        System.out.println("\n-------------------------------------");
        System.out.println("测试不满足完整分片时间的情况");
        String shortStartTime = "2025-03-11 19:38";
        String shortEndTime = "2025-03-11 19:40";
        System.out.println("将时间范围 " + shortStartTime + " 到 " + shortEndTime + " 按5分钟进行分片（只有2分钟）：");
        
        List<TimeShard> shortShards = DateShardingUtil.shardTimeRange(shortStartTime, shortEndTime, TimeUnit.MINUTE, 5);
        for (int i = 0; i < shortShards.size(); i++) {
            System.out.println("分片 " + (i + 1) + ": " + shortShards.get(i));
        }
        
        // 测试灵活的日期格式
        System.out.println("\n-------------------------------------");
        System.out.println("测试灵活的日期格式");
        String flexStartTime = "2025-3-11 19:38";  // 月份没有前导零
        String flexEndTime = "2025-3-11 23:38";    // 月份没有前导零
        System.out.println("将时间范围 " + flexStartTime + " 到 " + flexEndTime + " 按5分钟进行分片：");
        
        List<TimeShard> flexShards = DateShardingUtil.shardTimeRange(flexStartTime, flexEndTime, TimeUnit.MINUTE, 5);
        for (int i = 0; i < flexShards.size(); i++) {
            System.out.println("分片 " + (i + 1) + ": " + flexShards.get(i));
        }
        
        // 测试带有秒和毫秒的时间格式，但只保留到分钟
        System.out.println("\n-------------------------------------");
        System.out.println("测试带有秒和毫秒的时间格式（忽略秒和毫秒）");
        String timeWithSeconds = "2025-03-11 19:38:45";       // 带秒
        String timeWithMilliseconds = "2025-03-11 23:38:45.123"; // 带毫秒
        System.out.println("将时间范围 " + timeWithSeconds + " 到 " + timeWithMilliseconds + " 按5分钟进行分片：");
        
        List<TimeShard> timeWithSecondsShards = DateShardingUtil.shardTimeRange(timeWithSeconds, timeWithMilliseconds, TimeUnit.MINUTE, 5);
        for (int i = 0; i < timeWithSecondsShards.size(); i++) {
            System.out.println("分片 " + (i + 1) + ": " + timeWithSecondsShards.get(i));
        }
        
        // 测试周和月分片
        System.out.println("\n-------------------------------------");
        System.out.println("测试周和月分片");
        String weekStart = "2025-03-01 00:00";
        String weekEnd = "2025-03-30 00:00";
        
        System.out.println("将时间范围 " + weekStart + " 到 " + weekEnd + " 按1周进行分片：");
        shards = DateShardingUtil.shardTimeRange(weekStart, weekEnd, TimeUnit.WEEK, 1);
        for (int i = 0; i < shards.size(); i++) {
            System.out.println("分片 " + (i + 1) + ": " + shards.get(i));
        }
        
        System.out.println("\n将时间范围 " + weekStart + " 到 " + weekEnd + " 按1月进行分片：");
        shards = DateShardingUtil.shardTimeRange(weekStart, weekEnd, TimeUnit.MONTH, 1);
        for (int i = 0; i < shards.size(); i++) {
            System.out.println("分片 " + (i + 1) + ": " + shards.get(i));
        }
    }
}
