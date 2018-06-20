package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.ConsumptionInfo;
import com.migu.schedule.info.TaskInfo;

import java.util.*;

/*
*类名和方法不能修改
 */
public class Schedule {

    private TreeMap<Integer, ConsumptionInfo> taskIds = new TreeMap<Integer, ConsumptionInfo>();

    private TreeMap<Integer, TreeSet<ConsumptionInfo>> hangUpQueue = new TreeMap<Integer, TreeSet<ConsumptionInfo>>();

    private TreeMap<Integer, TreeSet<ConsumptionInfo>> nodeServers = new TreeMap<Integer, TreeSet<ConsumptionInfo>>();

    public int init() {
        taskIds = new TreeMap<Integer, ConsumptionInfo>();
        hangUpQueue = new TreeMap<Integer, TreeSet<ConsumptionInfo>>();
        nodeServers = new TreeMap<Integer, TreeSet<ConsumptionInfo>>();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {

        if (nodeId <= 0) {
            return ReturnCodeKeys.E004;
        }

        if (nodeServers.containsKey(nodeId)) {
            return ReturnCodeKeys.E005;
        }

        TreeSet<ConsumptionInfo> consumptionInfos = new TreeSet<ConsumptionInfo>();
        nodeServers.put(nodeId, consumptionInfos);
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {

        if (nodeId <= 0) {
            return ReturnCodeKeys.E004;
        }

        if (!nodeServers.containsKey(nodeId)) {
            return ReturnCodeKeys.E007;
        }

        TreeSet<ConsumptionInfo> consumptionInfos = nodeServers.remove(nodeId);

        Iterator<ConsumptionInfo> itr = consumptionInfos.iterator();
        while (itr.hasNext()) {
            ConsumptionInfo consumptionInfo = itr.next();
            consumptionInfo.setNodeId(-1);
            toHangUp(consumptionInfo);
        }
        return ReturnCodeKeys.E006;
    }

    private void toHangUp(ConsumptionInfo consumptionInfo) {

        TreeSet<ConsumptionInfo> consumptionInfos = hangUpQueue.get(consumptionInfo.getConsumption());
        if (null == consumptionInfos) {
            consumptionInfos = new TreeSet<ConsumptionInfo>();
            hangUpQueue.put(consumptionInfo.getConsumption(), consumptionInfos);
        }
        consumptionInfos.add(consumptionInfo);
    }


    public int addTask(int taskId, int consumption) {
        if (taskId <= 0) {
            return ReturnCodeKeys.E009;
        }

        if (taskIds.containsKey(taskId)) {
            return ReturnCodeKeys.E010;
        }
        ConsumptionInfo consumptionInfo = new ConsumptionInfo(taskId, consumption);
        taskIds.put(taskId, consumptionInfo);

        toHangUp(consumptionInfo);
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        if (taskId <= 0) {
            return ReturnCodeKeys.E009;
        }

        if (!taskIds.containsKey(taskId)) {
            return ReturnCodeKeys.E012;
        }

        ConsumptionInfo consumptionInfo = taskIds.remove(taskId);

        deleteTask(consumptionInfo, this.hangUpQueue);

        deleteTask(consumptionInfo, this.nodeServers);
        return ReturnCodeKeys.E011;
    }

    private void deleteTask(ConsumptionInfo consumptionInfo, TreeMap<Integer, TreeSet<ConsumptionInfo>> consumptionInfosMap) {
        Iterator<Map.Entry<Integer, TreeSet<ConsumptionInfo>>> itr = consumptionInfosMap.entrySet().iterator();
        while (itr.hasNext()) {
            itr.next().getValue().remove(consumptionInfo);
        }
    }

    public int scheduleTask(int threshold) {
        if (threshold <= 0) {
            return ReturnCodeKeys.E002;
        }

        for (ConsumptionInfo consumptionInfo : taskIds.values()) {
            toHangUp(consumptionInfo);
        }
        int nodeSize = nodeServers.size();
        int[] consumptions = new int[nodeSize];
        TreeSet<ConsumptionInfo>[] cpis = new TreeSet[nodeSize];
        for (int i = 0; i< nodeSize; i++) {
            cpis[i] = new TreeSet<ConsumptionInfo>();
        }

        TreeMap<Integer, TreeSet<ConsumptionInfo>> nodeServersBak = new TreeMap<Integer, TreeSet<ConsumptionInfo>>();
        int idx = 0;
        Collection<TreeSet<ConsumptionInfo>> cpiSetCol = hangUpQueue.values();
        for (TreeSet<ConsumptionInfo> cpiSet : cpiSetCol) {
            for (ConsumptionInfo cpi : cpiSet) {
                consumptions[idx] += cpi.getConsumption();
                cpis[idx].add(cpi);
                if (idx == nodeSize) {
                    idx = 0;
                }
            }
        }
        Arrays.sort(consumptions);

        if (consumptions[nodeSize -1] - consumptions[0] > threshold) {
            return ReturnCodeKeys.E014;
        }

        Iterator<Map.Entry<Integer, TreeSet<ConsumptionInfo>>> itr = nodeServers.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<Integer, TreeSet<ConsumptionInfo>> en = itr.next();
            Integer nodeId = en.getKey();
            TreeSet<ConsumptionInfo> consumptionInfosSet =  en.getValue();

        }

        return ReturnCodeKeys.E013;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        if (null == tasks) {
            return ReturnCodeKeys.E016;
        }
        tasks.clear();
        for (ConsumptionInfo consumptionInfo : taskIds.values()) {
            TaskInfo taskInfo = new TaskInfo();
            taskInfo.setNodeId(consumptionInfo.getNodeId());
            taskInfo.setTaskId(consumptionInfo.getTaskId());
            tasks.add(taskInfo);
        }
        return ReturnCodeKeys.E015;
    }
}
