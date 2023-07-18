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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import kubeclient.KubernetesPod;
import kubeclient.SeatunnelKubeClient;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/** Common utils for Kubernetes. */
public class KubernetesUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesUtils.class);

    private static final YAMLMapper yamlMapper = new YAMLMapper();

    private static final String LEADER_PREFIX = "org.apache.flink.k8s.leader.";
    private static final char LEADER_INFORMATION_SEPARATOR = ',';

    public static SeatunnelPod loadPodFromTemplateFile(
            SeatunnelKubeClient kubeClient, File podTemplateFile, String mainContainerName) {
        final KubernetesPod pod = kubeClient.loadPodFromTemplateFile(podTemplateFile);
        final List<Container> otherContainers = new ArrayList<>();
        Container mainContainer = null;

        if (null != pod.getInternalResource().getSpec()) {
            for (Container container : pod.getInternalResource().getSpec().getContainers()) {
                if (mainContainerName.equals(container.getName())) {
                    mainContainer = container;
                } else {
                    otherContainers.add(container);
                }
            }
            pod.getInternalResource().getSpec().setContainers(otherContainers);
        } else {
            // Set an empty spec for pod template
            pod.getInternalResource().setSpec(new PodSpecBuilder().build());
        }

        if (mainContainer == null) {
            LOG.info(
                    "Could not find main container {} in pod template, using empty one to initialize.",
                    mainContainerName);
            mainContainer = new ContainerBuilder().build();
        }

        return new SeatunnelPod(pod.getInternalResource(), mainContainer);
    }

    /** Generate name of the Deployment. */
    public static String getDeploymentName(String clusterId) {
        return clusterId;
    }
}
