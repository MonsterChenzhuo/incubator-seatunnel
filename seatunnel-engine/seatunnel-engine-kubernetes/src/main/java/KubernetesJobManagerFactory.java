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

import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import kubeclient.KubernetesJobManagerSpecification;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Utility class for constructing all the Kubernetes components on the client-side. This can include
 * the Deployment, the ConfigMap(s), and the Service(s).
 */
public class KubernetesJobManagerFactory {

    public static KubernetesJobManagerSpecification buildKubernetesJobManagerSpecification(
            SeatunnelPod podTemplate, KubernetesJobManagerParameters kubernetesJobManagerParameters)
            throws IOException {
        SeatunnelPod seatunnelPod = Preconditions.checkNotNull(podTemplate).copy();
        List<HasMetadata> accompanyingResources = new ArrayList<>();

        final List<KubernetesStepDecorator> stepDecorators = new ArrayList<>();
        for (KubernetesStepDecorator stepDecorator : stepDecorators) {
            seatunnelPod = stepDecorator.decorateFlinkPod(seatunnelPod);
            accompanyingResources.addAll(stepDecorator.buildAccompanyingKubernetesResources());
        }

        final Deployment deployment =
                createJobManagerDeployment(seatunnelPod, kubernetesJobManagerParameters);

        return new KubernetesJobManagerSpecification(deployment, accompanyingResources);
    }

    private static Deployment createJobManagerDeployment(
            SeatunnelPod seatunnelPod,
            KubernetesJobManagerParameters kubernetesJobManagerParameters) {
        final Container resolvedMainContainer = seatunnelPod.getMainContainer();

        final Pod resolvedPod =
                new PodBuilder(seatunnelPod.getPodWithoutMainContainer())
                        .editOrNewSpec()
                        .addToContainers(resolvedMainContainer)
                        .endSpec()
                        .build();

        return new DeploymentBuilder()
                .withApiVersion(Constants.APPS_API_VERSION)
                .editOrNewMetadata()
                .withName(
                        KubernetesUtils.getDeploymentName(
                                "kubernetesJobManagerParameters.getClusterId()"))
                .withAnnotations(new HashMap<>())
                .withLabels(new HashMap<>())
                .withOwnerReferences()
                .endMetadata()
                .editOrNewSpec()
                .withReplicas(1)
                .editOrNewTemplate()
                .withMetadata(resolvedPod.getMetadata())
                .withSpec(resolvedPod.getSpec())
                .endTemplate()
                .editOrNewSelector()
                .addToMatchLabels(new HashMap<>())
                .endSelector()
                .endSpec()
                .build();
    }
}
