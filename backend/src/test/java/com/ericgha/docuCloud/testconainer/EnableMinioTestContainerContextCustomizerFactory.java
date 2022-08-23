package com.ericgha.docuCloud.testconainer;

import com.ericgha.docuCloud.configuration.properties.AwsPropertiesKey;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.MapPropertySource;
import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;
import org.springframework.test.context.MergedContextConfiguration;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;

@Slf4j
public class EnableMinioTestContainerContextCustomizerFactory implements ContextCustomizerFactory {

    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @Inherited
    public @interface EnableMinioTestContainer {
    }

    @Override
    public ContextCustomizer createContextCustomizer(Class<?> testClass,
                                                     List<ContextConfigurationAttributes> configAttributes) {
        if (!( AnnotatedElementUtils.hasAnnotation( testClass, EnableMinioTestContainer.class ) )) {
            return null;
        }
        return new MinioTestContainerContextCustomizer();
    }

    @EqualsAndHashCode // See ContextCustomizer java doc
    private static class MinioTestContainerContextCustomizer implements ContextCustomizer {
        static final String IMAGE_TAG = "RELEASE.2022-08-22T23-53-06Z.fips";

        @Override
        public void customizeContext(ConfigurableApplicationContext context, MergedContextConfiguration mergedConfig) {
            var minioContainer = MinioContainer.builder()
                    .imageTag( IMAGE_TAG )
                    .accessKeyId( "admin" )
                    .secretAccessKey( "password" )
                    .build();
            minioContainer.start();
            var properties = Map.<String, Object>of(
                    AwsPropertiesKey.S3_ENDPOINT_OVERRRIDE.get(), minioContainer.getApiAddress(),
                    AwsPropertiesKey.ACCESS_KEY_ID.get(), minioContainer.getAccessKeyId(),
                   AwsPropertiesKey.SECRET_ACCESS_KEY.get(), minioContainer.getSecretAccessKey()
            );
            log.info( "Minio contianer ready to start Web Address: {}", minioContainer.getWebAddress() );
            log.info( "Minio container ready to start with Username: {} and Password: {}", minioContainer.getAccessKeyId(), minioContainer.getSecretAccessKey() );
            var propertySource = new MapPropertySource( "MinioContainer Test Properties", properties );
            context.getEnvironment().getPropertySources().addFirst( propertySource );
        }

    }

}
