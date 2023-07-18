import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import configuration.KubernetesConfigOptions;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import kubeclient.Fabric8SeatunnelKubeClient;
import kubeclient.SeatunnelKubeClient;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** A {@link SeatunnelKubeClientFactory} for creating the {@link SeatunnelKubeClient}. */
public class SeatunnelKubeClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SeatunnelKubeClientFactory.class);

    private static final SeatunnelKubeClientFactory INSTANCE = new SeatunnelKubeClientFactory();

    public static SeatunnelKubeClientFactory getInstance() {
        return INSTANCE;
    }

    /**
     * Create a Flink Kubernetes client with the given configuration.
     *
     * @param flinkConfig Flink configuration
     * @param useCase Flink Kubernetes client use case (e.g. client, resourcemanager,
     *     kubernetes-ha-services)
     * @return Return the Flink Kubernetes client with the specified configuration and dedicated IO
     *     executor.
     */
    public SeatunnelKubeClient fromConfiguration(Configuration flinkConfig, String useCase) {
        final Config config;

        final String kubeContext = flinkConfig.getString(KubernetesConfigOptions.CONTEXT);
        if (kubeContext != null) {
            LOG.info("Configuring kubernetes client to use context {}.", kubeContext);
        }

        final String kubeConfigFile =
                flinkConfig.getString(KubernetesConfigOptions.KUBE_CONFIG_FILE);
        if (kubeConfigFile != null) {
            LOG.debug("Trying to load kubernetes config from file: {}.", kubeConfigFile);
            try {
                // If kubeContext is null, the default context in the kubeConfigFile will be used.
                // Note: the third parameter kubeconfigPath is optional and is set to null. It is
                // only used to rewrite
                // relative tls asset paths inside kubeconfig when a file is passed, and in the case
                // that the kubeconfig
                // references some assets via relative paths.
                config =
                        Config.fromKubeconfig(
                                kubeContext,
                                FileUtils.readFileUtf8(new File(kubeConfigFile)),
                                null);
            } catch (IOException e) {
                throw new KubernetesClientException("Load kubernetes config failed.", e);
            }
        } else {
            LOG.debug("Trying to load default kubernetes config.");

            config = Config.autoConfigure(kubeContext);
        }

        final String namespace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
        final String userAgent =
                flinkConfig.getString(KubernetesConfigOptions.KUBERNETES_CLIENT_USER_AGENT);
        config.setNamespace(namespace);
        config.setUserAgent(userAgent);
        LOG.debug("Setting Kubernetes client namespace: {}, userAgent: {}", namespace, userAgent);

        final NamespacedKubernetesClient client = new DefaultKubernetesClient(config);
        final int poolSize =
                flinkConfig.get(KubernetesConfigOptions.KUBERNETES_CLIENT_IO_EXECUTOR_POOL_SIZE);
        return new Fabric8SeatunnelKubeClient(
                flinkConfig, client, createThreadPoolForAsyncIO(poolSize, useCase));
    }

    private static ExecutorService createThreadPoolForAsyncIO(int poolSize, String useCase) {
        return Executors.newFixedThreadPool(
                poolSize, new ExecutorThreadFactory("flink-kubeclient-io-for-" + useCase));
    }
}
