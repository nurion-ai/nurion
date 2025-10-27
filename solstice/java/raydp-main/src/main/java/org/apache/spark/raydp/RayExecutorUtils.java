/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.raydp;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PlacementGroups;
import io.ray.api.Ray;
import io.ray.api.call.ActorCreator;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
// import io.ray.api.scheduling.SchedulingStrategy;
import org.apache.spark.executor.RayDPExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RayExecutorUtils {

    public static final Logger logger = LoggerFactory.getLogger(RayExecutorUtils.class);

    /**
     * Convert from mbs -> memory units. The memory units in ray is byte
     */
    private static double toMemoryUnits(int memoryInMB) {
        double result = 1.0 * memoryInMB * 1024 * 1024;
        return Math.round(result);
    }

    public static String getExecutorActorName(String appName, String executorId) {
        return "raydp-executor-" + appName + "-" + executorId;
    }

    public static ActorHandle<RayDPExecutor> createExecutorActor(
        String appName,
        String executorId,
        String appMasterURL,
        double cores,
        int memoryInMB,
        Map<String, Double> resources,
        List<String> javaOpts,
        boolean usePlacementGroup,
        String schedulingStrategy
    ) {
        String executorActorName = getExecutorActorName(appName, executorId);

        ActorCreator<RayDPExecutor> creator = Ray.actor(
            RayDPExecutor::new, appName, executorId, appMasterURL);
        creator.setName(executorActorName);
        creator.setJvmOptions(javaOpts);
        // creator.setSchedulingStrategy(SchedulingStrategy.fromString(schedulingStrategy));
        if (usePlacementGroup) {
            Map<String, Double> bundle = new HashMap<>() {{
                put("CPU", cores);
                put("memory", toMemoryUnits(memoryInMB));
            }};
            bundle.putAll(resources);
            PlacementGroup pg = PlacementGroups.createPlacementGroup(new PlacementGroupCreationOptions.Builder()
                .setBundles(List.of(bundle))
                .setName(executorActorName + "-pg")
                .setStrategy(PlacementStrategy.SPREAD)
                .build());
            creator.setPlacementGroup(pg);
        } else {
            creator.setResource("CPU", cores);
            creator.setResource("memory", toMemoryUnits(memoryInMB));
            for (Map.Entry<String, Double> entry : resources.entrySet()) {
                creator.setResource(entry.getKey(), entry.getValue());
            }
        }

        creator.setMaxConcurrency(2);
        return creator.remote();
    }

    public static void setUpExecutor(
        ActorHandle<RayDPExecutor> handler,
        String driverUrl,
        int cores,
        String classPathEntries) {
        handler.task(RayDPExecutor::startUp, driverUrl, cores, classPathEntries).remote();
    }

    public static ObjectRef<String> heartbeat(
        ActorHandle<RayDPExecutor> handler) {
        return handler.task(RayDPExecutor::heartbeat).remote();
    }

    public static ObjectRef<Integer> runningTasks(ActorHandle<RayDPExecutor> handler) {
        return handler.task(RayDPExecutor::numRunningTasks).remote();
    }

    public static String[] getBlockLocations(
        ActorHandle<RayDPExecutor> handler,
        int rddId,
        int numPartitions) {
        return handler.task(RayDPExecutor::getBlockLocations,
            rddId, numPartitions).remote().get();
    }

    public static ObjectRef<byte[]> getRDDPartition(
        ActorHandle<RayDPExecutor> handle,
        int rddId,
        int partitionId,
        String schema,
        String driverAgentUrl) {
        return handle.task(
            RayDPExecutor::getRDDPartition,
            rddId, partitionId, schema, driverAgentUrl).remote();
    }

    public static void exitExecutor(ActorHandle<RayDPExecutor> handle, String appName, String executorId) {
        handle.kill(true);
        PlacementGroup pg = PlacementGroups.getPlacementGroup(getExecutorActorName(appName, executorId) + "-pg");
        if (pg != null) {
            PlacementGroups.removePlacementGroup(pg.getId());
        }
    }

}
