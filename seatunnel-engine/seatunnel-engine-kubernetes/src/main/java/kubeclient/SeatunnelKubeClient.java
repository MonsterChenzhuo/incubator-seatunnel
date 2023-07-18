/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kubeclient;

import kubeclient.resource.KubernetesWatch;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * 这个代码定义了一个名为FlinkKubeClient的接口，它定义了与Kubernetes交互的一系列方法。 接口中的所有方法都将在Client和ResourceManager中被调用。
 * 为了避免可能阻塞RpcEndpoint的主线程的执行，createTaskManagerPod(KubernetesPod)和stopPod(String)等方法应异步实现。
 */
public interface SeatunnelKubeClient extends AutoCloseable {

    /** 创建Master组件，这可能包括Deployment，ConfigMap(s)和Service(s)。 */
    void createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec);

    /** 创建Task Manager pod，返回创建pod的Future。 */
    CompletableFuture<Void> createTaskManagerPod(KubernetesPod kubernetesPod);

    /** 停止指定名称的pod，返回停止pod的Future */
    CompletableFuture<Void> stopPod(String podName);

    /** 停止集群并清理所有资源，包括服务，辅助服务和所有正在运行的pods。 */
    void stopAndCleanupCluster(String clusterId);

    /** 获取给定Flink集群Id的Kubernetes服务。 */
    Optional<KubernetesService> getService(String serviceName);

    /** 获取集群外部访问的rest端点。 */
    Optional<Endpoint> getRestEndpoint(String clusterId);

    /** 列出带有指定标签的pods。 */
    List<KubernetesPod> getPodsWithLabels(Map<String, String> labels);

    /** 监视选定的pods并执行回调。 */
    KubernetesWatch watchPodsAndDoCallback(
            Map<String, String> labels, WatchCallbackHandler<KubernetesPod> podCallbackHandler)
            throws Exception;

    /** Close the Kubernetes client with no exception. */
    void close();

    /** 从模板文件加载pod。 */
    KubernetesPod loadPodFromTemplateFile(File podTemplateFile);

    /** 更新给定Kubernetes服务的目标端口 */
    CompletableFuture<Void> updateServiceTargetPort(
            String serviceName, String portName, int targetPort);

    /** 回调接口，用于处理Kubernetes资源的增加、修改、删除和错误情况。 */
    interface WatchCallbackHandler<T> {

        void onAdded(List<T> resources);

        void onModified(List<T> resources);

        void onDeleted(List<T> resources);

        void onError(List<T> resources);

        void handleError(Throwable throwable);
    }
}
