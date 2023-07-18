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

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * 它表示一个由JobManager/TaskManager Pod组成的Flink Pod Flink Pod中可能包含Pod本身、主容器（Main Container）、初始化容器（Init
 * Container）等。 它有两个主要的成员变量：podWithoutMainContainer和mainContainer。
 */
public class SeatunnelPod {

    private final Pod podWithoutMainContainer;

    private final Container mainContainer;

    public SeatunnelPod(Pod podWithoutMainContainer, Container mainContainer) {
        this.podWithoutMainContainer = podWithoutMainContainer;
        this.mainContainer = mainContainer;
    }

    public Pod getPodWithoutMainContainer() {
        return podWithoutMainContainer;
    }

    public Container getMainContainer() {
        return mainContainer;
    }

    public SeatunnelPod copy() {
        return new SeatunnelPod(
                new PodBuilder(this.getPodWithoutMainContainer()).build(),
                new ContainerBuilder(this.getMainContainer()).build());
    }

    /** Builder for creating a {@link SeatunnelPod}. */
    public static class Builder {

        private Pod podWithoutMainContainer;
        private Container mainContainer;

        public Builder() {
            this.podWithoutMainContainer =
                    new PodBuilder()
                            .withNewMetadata()
                            .endMetadata()
                            .withNewSpec()
                            .endSpec()
                            .build();

            this.mainContainer = new ContainerBuilder().build();
        }

        public Builder(SeatunnelPod seatunnelPod) {
            checkNotNull(seatunnelPod);
            this.podWithoutMainContainer = checkNotNull(seatunnelPod.getPodWithoutMainContainer());
            this.mainContainer = checkNotNull(seatunnelPod.getMainContainer());
        }

        public Builder withPod(Pod pod) {
            this.podWithoutMainContainer = checkNotNull(pod);
            return this;
        }

        public Builder withMainContainer(Container mainContainer) {
            this.mainContainer = checkNotNull(mainContainer);
            return this;
        }

        public SeatunnelPod build() {
            return new SeatunnelPod(this.podWithoutMainContainer, this.mainContainer);
        }
    }
}
