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

import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import configuration.KubernetesConfigOptions;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import kubeclient.resource.KubernetesWatch;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The implementation of {@link SeatunnelKubeClient}. */
public class Fabric8SeatunnelKubeClient implements SeatunnelKubeClient {

    private static final Logger LOG = LoggerFactory.getLogger(Fabric8SeatunnelKubeClient.class);

    private final String clusterId;
    private final String namespace;
    private final int maxRetryAttempts;
//    private final KubernetesConfigOptions.NodePortAddressType nodePortAddressType;

    // internalClient是一个内部的Kubernetes客户端，用于执行与Kubernetes API服务器的实际交互操作。
    private final NamespacedKubernetesClient internalClient;
    private final ExecutorService kubeClientExecutorService;
    // save the master deployment atomic reference for setting owner reference of task manager pods
    private final AtomicReference<Deployment> masterDeploymentRef;

    public Fabric8SeatunnelKubeClient(
            Configuration flinkConfig,
            NamespacedKubernetesClient client,
            ExecutorService executorService) {
        this.clusterId =
                flinkConfig
                        .getOptional(KubernetesConfigOptions.CLUSTER_ID)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                String.format(
                                                        "Configuration option '%s' is not set.",
                                                        KubernetesConfigOptions.CLUSTER_ID.key())));
        this.namespace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
        this.maxRetryAttempts =
                flinkConfig.getInteger(
                        KubernetesConfigOptions.KUBERNETES_TRANSACTIONAL_OPERATION_MAX_RETRIES);
//        this.nodePortAddressType =
//                flinkConfig.get(
//                        KubernetesConfigOptions.REST_SERVICE_EXPOSED_NODE_PORT_ADDRESS_TYPE);
        this.internalClient = checkNotNull(client);
        this.kubeClientExecutorService = checkNotNull(executorService);
        this.masterDeploymentRef = new AtomicReference<>();
    }

    @Override
    public void createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec) {
        // 首先，从传入的KubernetesJobManagerSpecification对象中获取Deployment对象和其他附带资源(accompanyingResources)。
        final Deployment deployment = kubernetesJMSpec.getDeployment();
        final List<HasMetadata> accompanyingResources = kubernetesJMSpec.getAccompanyingResources();

        // 创建一个新的Deployment。在创建之前，会通过日志记录要创建的Deployment的详细信息。
        final Deployment createdDeployment =
                this.internalClient.apps().deployments().create(deployment);

        /**
         * 创建了Deployment之后，通过调用setOwnerReference方法，将创建的Deployment设置为其他附带资源的Owner Reference。
         * 这意味着当Deployment被删除时，Kubernetes会自动清理与其相关联的所有资源。
         */
        setOwnerReference(createdDeployment, accompanyingResources);
        /**
         * createOrReplace()。这个方法会遍历列表中的所有资源，并尝试创建它们。如果相应的资源已经存在，那么它将被替换。 这就是 createOrReplace()
         * 方法的含义。所以这行代码的作用是，尝试创建列表中的所有资源，如果相应资源已存在，则替换它。
         */
        this.internalClient.resourceList(accompanyingResources).createOrReplace();
    }

    /**
     * 在这个方法中，首先创建一个指向 Deployment 的所有者引用。然后，这个引用被添加到资源列表中的每一个资源的元数据中。 这样做的结果是，如果 Deployment 被删除，那么
     * Kubernetes 的垃圾回收机制会自动删除所有属于这个 Deployment 的资源。
     */
    private void setOwnerReference(Deployment deployment, List<HasMetadata> resources) {
        final OwnerReference deploymentOwnerReference =
                new OwnerReferenceBuilder()
                        .withName(deployment.getMetadata().getName())
                        .withApiVersion(deployment.getApiVersion())
                        .withUid(deployment.getMetadata().getUid())
                        .withKind(deployment.getKind())
                        .withController(true)
                        .withBlockOwnerDeletion(true)
                        .build();
        resources.forEach(
                resource ->
                        resource.getMetadata()
                                .setOwnerReferences(
                                        Collections.singletonList(deploymentOwnerReference)));
    }

    @Override
    public CompletableFuture<Void> createTaskManagerPod(KubernetesPod kubernetesPod) {
        return null;
    }

    @Override
    public CompletableFuture<Void> stopPod(String podName) {
        return null;
    }

    @Override
    public void stopAndCleanupCluster(String clusterId) {}

    @Override
    public Optional<KubernetesService> getService(String serviceName) {
        return Optional.empty();
    }

    @Override
    public Optional<Endpoint> getRestEndpoint(String clusterId) {
        return Optional.empty();
    }

    @Override
    public List<KubernetesPod> getPodsWithLabels(Map<String, String> labels) {
        return null;
    }

    @Override
    public KubernetesWatch watchPodsAndDoCallback(
            Map<String, String> labels, WatchCallbackHandler<KubernetesPod> podCallbackHandler)
            throws Exception {
        return null;
    }

    @Override
    public void close() {}

    @Override
    public KubernetesPod loadPodFromTemplateFile(File podTemplateFile) {
        return null;
    }

    @Override
    public CompletableFuture<Void> updateServiceTargetPort(
            String serviceName, String portName, int targetPort) {
        return null;
    }
}
