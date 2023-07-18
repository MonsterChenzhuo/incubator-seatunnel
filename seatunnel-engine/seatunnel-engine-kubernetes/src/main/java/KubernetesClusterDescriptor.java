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

import org.apache.flink.configuration.Configuration;

import cluster.ClusterClientProvider;
import cluster.ClusterDescriptor;
import cluster.ClusterSpecification;
import kubeclient.KubernetesJobManagerSpecification;
import kubeclient.SeatunnelKubeClient;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkNotNull;

/** Kubernetes specific {@link ClusterDescriptor} implementation. */
@Slf4j
public class KubernetesClusterDescriptor implements ClusterDescriptor<String> {
    private static final String CLUSTER_DESCRIPTION = "Kubernetes cluster";

    private final Configuration zetaConfig;

    private final SeatunnelKubeClient client;

    private final String clusterId;

    public KubernetesClusterDescriptor(Configuration zetaConfig, SeatunnelKubeClient client) {
        this.zetaConfig = zetaConfig;
        this.client = client;
        this.clusterId = checkNotNull(("CLUSTER_ID"), "ClusterId must be specified!");
    }

    @Override
    public String getClusterDescription() {
        return CLUSTER_DESCRIPTION;
    }

    /**
     * 用于部署 Flink session集群。 接收一个 cluster.ClusterSpecification 对象，表示要部署的集群规格。 它将部署一个会话集群，并返回一个
     * cluster.ClusterClientProvider<String> 对象。
     */
    @Override
    public ClusterClientProvider<String> deploySessionCluster(
            ClusterSpecification clusterSpecification) throws Exception {

        return deployClusterInternal();
    }

    /** 该方法负责在Kubernetes上实际部署Flink集群 */
    private ClusterClientProvider<String> deployClusterInternal()
            throws Exception {

        try {
            final KubernetesJobManagerParameters kubernetesJobManagerParameters =
                    new KubernetesJobManagerParameters();
            SeatunnelPod podTemplate = new SeatunnelPod.Builder().build();
            // 使用Pod模板和KubernetesJobManagerParameters构建KubernetesJobManagerSpecification。
            final KubernetesJobManagerSpecification kubernetesJobManagerSpec =
                    KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                            podTemplate, kubernetesJobManagerParameters);
            // 调用createJobManagerComponent方法来创建组件。
            client.createJobManagerComponent(kubernetesJobManagerSpec);
            // 创建并返回一个ClusterClientProvider。如果在创建过程中发生异常，将尝试停止并清理集群，并抛出ClusterDeploymentException。
            return null;
        } catch (Exception e) {
            try {
                log.warn(
                        "Failed to create the Kubernetes cluster \"{}\", try to clean up the residual resources.",
                        clusterId);
                client.stopAndCleanupCluster(clusterId);
            } catch (Exception e1) {
                log.info(
                        "Failed to stop and clean up the Kubernetes cluster \"{}\".",
                        clusterId,
                        e1);
            }
            throw new Exception("Could not create Kubernetes cluster \"" + clusterId + "\".", e);
        }
    }

    @Override
    public void killCluster(String clusterId) throws Exception {
        try {
            client.stopAndCleanupCluster(clusterId);
        } catch (Exception e) {
            throw new Exception("Could not kill Kubernetes cluster " + clusterId);
        }
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (Exception e) {
            log.error("failed to close client, exception {}", e.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        KubernetesClusterDescriptor kubernetesClusterDescriptor =
                new KubernetesClusterDescriptor(
                        configuration,
                        SeatunnelKubeClientFactory.getInstance()
                                .fromConfiguration(configuration, "client"));

        kubernetesClusterDescriptor.deploySessionCluster(null);
        //
        // kubernetesClusterDescriptor.deploySessionCluster(kubernetesClusterClientFactory.getClusterSpecification(
        //                configuration));
    }
}
