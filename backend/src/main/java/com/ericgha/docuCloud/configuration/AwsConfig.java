package com.ericgha.docuCloud.configuration;

import com.ericgha.docuCloud.configuration.properties.AwsPropertiesKey;
import com.ericgha.docuCloud.service.AppEnvPropertiesService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.Bucket;

import java.net.URI;
import java.security.Security;
import java.time.Duration;
import java.util.Objects;

@Slf4j
public class AwsConfig {

    @Autowired
    AppEnvPropertiesService propertiesService;

    static {
        Security.setProperty("networkaddress.cache.ttl" , "60");
    }

    AwsCredentialsProvider awsCredentials() {
        String accessKey = propertiesService.get( AwsPropertiesKey.ACCESS_KEY_ID );
        String secretKey = propertiesService.get( AwsPropertiesKey.SECRET_ACCESS_KEY );
        AwsBasicCredentials credentials = AwsBasicCredentials.create( accessKey, secretKey );
        return StaticCredentialsProvider.create( credentials );
    }

    @Bean
    S3AsyncClient asyncClient() {
        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder()
                .writeTimeout( Duration.ZERO)
                .maxConcurrency(64)
                .build();

        S3Configuration s3Config = S3Configuration.builder()
                .checksumValidationEnabled(true)
                .chunkedEncodingEnabled(true)
                .build();

        S3AsyncClientBuilder builder = S3AsyncClient.builder()
                .httpClient( httpClient )
                .region( Region.of(propertiesService.get( AwsPropertiesKey.REGION ) ) )
                .serviceConfiguration( s3Config )
                .credentialsProvider( awsCredentials() );
        try {
            String overrideEndpoint = propertiesService.get( AwsPropertiesKey.S3_ENDPOINT_OVERRRIDE );
            builder.endpointOverride( URI.create( overrideEndpoint ) );
            log.info("Overriding S3 endpoint with: {}", overrideEndpoint);
        } catch (NullPointerException e) {
            log.debug("Using default s3 endpoint.");
        }
        return builder.build();
    }

    @Bean("ROOT")
    Bucket rootBucket() {
        return Bucket.builder().name(propertiesService.get( AwsPropertiesKey.S3_BUCKET ) ).build();
    }
}
