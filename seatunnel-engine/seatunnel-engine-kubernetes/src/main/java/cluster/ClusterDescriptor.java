package cluster;

public interface ClusterDescriptor<T> extends AutoCloseable {

    /** 返回一个字符串，包含有关集群的详细信息（例如NodeManagers，可用内存等）。 */
    String getClusterDescription();

    /** 触发集群的部署，参数是定义要部署的集群的集群规范，返回的是部署好的集群的客户端。 */
    ClusterClientProvider<T> deploySessionCluster(ClusterSpecification clusterSpecification)
            throws Exception;

    /** 终止具有给定集群ID的集群 */
    void killCluster(T clusterId) throws Exception;

    @Override
    void close();
}
