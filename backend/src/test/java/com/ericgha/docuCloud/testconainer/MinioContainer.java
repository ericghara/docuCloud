package com.ericgha.docuCloud.testconainer;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

@Slf4j
public class MinioContainer extends GenericContainer<MinioContainer> {

    private static final int API_PORT = 9000;
    private static final int WEB_PORT = 9001;
    private static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    private static final String MINIO_HEALTH_ENDPOINT = "/minio/health/ready";
    private static final String USER_ENV_KEY = "MINIO_ROOT_USER";
    private static final String PASSWORD_ENV_KEY = "MINIO_ROOT_PASSWORD";

    // You probably want to update this
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("quay.io/minio/minio");

    @Builder
    public MinioContainer(@Nullable String accessKeyId,
                          @Nullable String secretAccessKey, @Nullable String imageTag) {
        super(DEFAULT_IMAGE_NAME.withTag(Objects.nonNull(imageTag) ? imageTag : "") );
        addExposedPorts( API_PORT );
        addExposedPorts( WEB_PORT );
        if (Objects.isNull(accessKeyId) ) {
             accessKeyId = UUID.randomUUID().toString();
        }
        if (Objects.isNull(secretAccessKey) ) {
            secretAccessKey = UUID.randomUUID().toString();
        }

        super.withEnv( USER_ENV_KEY, accessKeyId );
        super.withEnv( PASSWORD_ENV_KEY, secretAccessKey );
        super.withCommand("server", DEFAULT_STORAGE_DIRECTORY, "--console-address", ":" + WEB_PORT );
        setWaitStrategy(new HttpWaitStrategy()
                .forPort( API_PORT )
                .forPath(MINIO_HEALTH_ENDPOINT)
                .withStartupTimeout( Duration.ofMinutes(1)));
    }

    public String getApiAddress() {
        return String.format("http://%s:%d", getHost(), getMappedPort( API_PORT ) );
    }

    public String getWebAddress() {
        //noinspection HttpUrlsUsage
        return String.format("http://%s:%d", getHost(), getMappedPort( WEB_PORT ) );
    }

    public String getAccessKeyId() {
        // note EnvMap looks mutable
        return super.getEnvMap().get(USER_ENV_KEY);
    }

    public String getSecretAccessKey() {
        return super.getEnvMap().get(PASSWORD_ENV_KEY);
    }
}
