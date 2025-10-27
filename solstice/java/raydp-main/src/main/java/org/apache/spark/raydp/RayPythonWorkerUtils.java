package org.apache.spark.raydp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorMethod;
import io.ray.runtime.actor.NativePyActorHandle;
import org.apache.spark.SparkEnv;

import java.util.Map;
import java.util.Optional;

public class RayPythonWorkerUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static PyActorHandle create(String executorId, Map<String, String> envs) throws JsonProcessingException {
        String currentNodeId = Ray.getRuntimeContext().getCurrentNodeId().toString();
        String appName = SparkEnv.get().conf().get("spark.app.name");
        String masterName = appName + "_SPARK_MASTER";
        Optional<BaseActorHandle> maybeMaster = Ray.getActor(masterName);
        NativePyActorHandle pyMasterHandle = (NativePyActorHandle) maybeMaster.orElseThrow();
        ObjectRef<NativePyActorHandle> rst = pyMasterHandle.task(
                PyActorMethod.of("create_pyworker", NativePyActorHandle.class),
                executorId, currentNodeId, objectMapper.writeValueAsString(envs)).remote();
        return Ray.get(rst);
    }

    public static int getPort(PyActorHandle handle) {
        ObjectRef<Object> ref = handle.task(PyActorMethod.of("get_port")).remote();
        return (int) Ray.get(ref);
    }

    public static void start(PyActorHandle handle) {
        handle.task(PyActorMethod.of("start")).remote();
    }
}
