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

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;

import java.util.List;

/**
 * 表示一个Flink应用在Kubernetes上的组件。它包含两个主要的成员变量：deployment和accompanyingResources。
 * deployment是一个Deployment类型的对象，表示Kubernetes上的Deployment资源，用于描述所需的应用状态，Kubernetes会自动将实际状态调整为所需状态。
 *
 * <p>accompanyingResources是一个List<HasMetadata>类型的对象，包含与Deployment相关联的其他Kubernetes资源，HasMetadata是所有Kubernetes资源对象的共有接口，它包含了获取资源元数据（如名称、命名空间等）的方法。
 * 这个类提供了一个用于封装Kubernetes部署以及伴随资源的数据结构，方便进行Flink应用的部署和管理。
 */
public class KubernetesJobManagerSpecification {

    private Deployment deployment;

    private List<HasMetadata> accompanyingResources;

    public KubernetesJobManagerSpecification(
            Deployment deployment, List<HasMetadata> accompanyingResources) {
        this.deployment = deployment;
        this.accompanyingResources = accompanyingResources;
    }

    public Deployment getDeployment() {
        return deployment;
    }

    public List<HasMetadata> getAccompanyingResources() {
        return accompanyingResources;
    }
}
