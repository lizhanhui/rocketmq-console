package com.alibaba.rocketmq.service;

import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.offset.CloneGroupOffsetCommand;
import com.alibaba.rocketmq.tools.command.offset.ResetOffsetByTimeCommand;
import com.alibaba.rocketmq.validate.CmdTrace;
import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.alibaba.rocketmq.common.Tool.bool;
import static com.alibaba.rocketmq.common.Tool.str;
import static org.apache.commons.lang.StringUtils.isNotBlank;


/**
 * @see com.alibaba.rocketmq.tools.command.offset.ResetOffsetByTimeOldCommand
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-19
 */
@Service
public class OffsetService extends AbstractService {

    static final Logger logger = LoggerFactory.getLogger(OffsetService.class);

    static final ResetOffsetByTimeCommand resetOffsetByTimeSubCommand = new ResetOffsetByTimeCommand();


    public Collection<Option> getOptionsForResetOffsetByTime() {
        return getOptions(resetOffsetByTimeSubCommand);
    }


    @CmdTrace(cmdClazz = ResetOffsetByTimeCommand.class)
    public Table resetOffsetByTime(String consumerGroup, String broker, String topic, String timeStampStr, String forceStr)
            throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            long timestamp = 0;
            try {
                // 直接输入 long 类型的 timestamp
                timestamp = Long.valueOf(timeStampStr);
            }
            catch (NumberFormatException e) {
                // 输入的为日期格式，精确到毫秒
                timestamp = UtilAll.parseDate(timeStampStr, UtilAll.yyyy_MM_dd_HH_mm_ss_SSS).getTime();
            }

            boolean force = true;
            if (isNotBlank(forceStr)) {
                force = bool(forceStr.trim());
            }
            defaultMQAdminExt.start();
            Map<MessageQueue, Long> rollbackStatsList =
                    defaultMQAdminExt.resetOffsetByTimestamp(topic, broker, consumerGroup, timestamp, force);

            // System.out
            // .printf(
            // "rollback consumer offset by specified consumerGroup[%s], topic[%s], force[%s], timestamp(string)[%s], timestamp(long)[%s]\n",
            // consumerGroup, topic, force, timeStampStr, timestamp);
            //
            // System.out.printf("%-20s  %-20s  %-20s  %-20s  %-20s  %-20s\n",//
            // "#brokerName",//
            // "#queueId",//
            // "#brokerOffset",//
            // "#consumerOffset",//
            // "#timestampOffset",//
            // "#rollbackOffset" //
            // );
            String[] thead =
                    new String[] { "#brokerName", "#queueId", "#rollbackOffset" };
            Table table = new Table(thead, rollbackStatsList.size());
            Set<Entry<MessageQueue, Long>> entrys = rollbackStatsList.entrySet();
            for (Entry<MessageQueue, Long> entry : entrys) {
                // System.out.printf("%-20s  %-20d  %-20d  %-20d  %-20d  %-20d\n",//
                // UtilAll.frontStringAtLeast(rollbackStats.getBrokerName(),
                // 32),//
                // rollbackStats.getQueueId(),//
                // rollbackStats.getBrokerOffset(),//
                // rollbackStats.getConsumerOffset(),//
                // rollbackStats.getTimestampOffset(),//
                // rollbackStats.getRollbackOffset() //
                // );
                Object[] tr = table.createTR();
                tr[0] = UtilAll.frontStringAtLeast(  entry.getKey().getBrokerName() , 64);
                tr[1] = str(entry.getKey().getQueueId());
                tr[2] = str(entry.getValue());
                table.insertTR(tr);
            }
            return table;
        }
        catch (Throwable e) {
            logger.error(e.getMessage(), e);
            t = e;
        }
        finally {
            shutdownDefaultMQAdminExt(defaultMQAdminExt);
        }
        throw t;
    }

    @CmdTrace(cmdClazz = CloneGroupOffsetCommand.class)
    public Table cloneGroupOffSet() {
        throw new UnsupportedOperationException("Not Supported Now");
    }

    @CmdTrace(cmdClazz = ResetOffsetByTimeCommand.class)
    public Table resetOffSetByTime() {
        throw new UnsupportedOperationException("Not Implemented");
    }
}
