package cluster;

/** Description of the cluster to start by the {@link ClusterDescriptor}. */
public final class ClusterSpecification {
    private final int masterMemoryMB;
    private final int taskManagerMemoryMB;
    private final int slotsPerTaskManager;

    private ClusterSpecification(
            int masterMemoryMB, int taskManagerMemoryMB, int slotsPerTaskManager) {
        this.masterMemoryMB = masterMemoryMB;
        this.taskManagerMemoryMB = taskManagerMemoryMB;
        this.slotsPerTaskManager = slotsPerTaskManager;
    }

    public int getMasterMemoryMB() {
        return masterMemoryMB;
    }

    public int getTaskManagerMemoryMB() {
        return taskManagerMemoryMB;
    }

    public int getSlotsPerTaskManager() {
        return slotsPerTaskManager;
    }

    @Override
    public String toString() {
        return "cluster.ClusterSpecification{"
                + "masterMemoryMB="
                + masterMemoryMB
                + ", taskManagerMemoryMB="
                + taskManagerMemoryMB
                + ", slotsPerTaskManager="
                + slotsPerTaskManager
                + '}';
    }

    /** Builder for the {@link ClusterSpecification} instance. */
    public static class ClusterSpecificationBuilder {
        private int masterMemoryMB = 768;
        private int taskManagerMemoryMB = 1024;
        private int slotsPerTaskManager = 1;

        public ClusterSpecificationBuilder setMasterMemoryMB(int masterMemoryMB) {
            this.masterMemoryMB = masterMemoryMB;
            return this;
        }

        public ClusterSpecificationBuilder setTaskManagerMemoryMB(int taskManagerMemoryMB) {
            this.taskManagerMemoryMB = taskManagerMemoryMB;
            return this;
        }

        public ClusterSpecificationBuilder setSlotsPerTaskManager(int slotsPerTaskManager) {
            this.slotsPerTaskManager = slotsPerTaskManager;
            return this;
        }

        public ClusterSpecification createClusterSpecification() {
            return new ClusterSpecification(
                    masterMemoryMB, taskManagerMemoryMB, slotsPerTaskManager);
        }
    }
}
