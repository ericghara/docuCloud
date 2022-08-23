package com.ericgha.docuCloud.testconainer;

import lombok.EqualsAndHashCode;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.MapPropertySource;
import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;
import org.springframework.test.context.MergedContextConfiguration;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;


public class EnablePostgresTestContainerContextCustomizerFactory implements ContextCustomizerFactory {

    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @Inherited
    public @interface EnabledPostgresTestContainer {
    }

    @Override
    public ContextCustomizer createContextCustomizer(Class<?> testClass,
                                                     List<ContextConfigurationAttributes> configAttributes) {
        if (!( AnnotatedElementUtils.hasAnnotation( testClass, EnabledPostgresTestContainer.class ) )) {
            return null;
        }
        return new PostgresTestContainerContextCustomizer();
    }

    @EqualsAndHashCode // See ContextCustomizer java doc
    private static class PostgresTestContainerContextCustomizer implements ContextCustomizer {

        private static final DockerImageName image = DockerImageName
                .parse( "postgres" )
                .withTag( "14" );

        @Override
        public void customizeContext(ConfigurableApplicationContext context, MergedContextConfiguration mergedConfig) {
            var postgresContainer = new PostgreSQLContainer<>( DockerImageName.parse( "postgres:14" ) )
                    .withDatabaseName( "docu-cloud-test-db" )
                    .withUsername( "admin" )
                    .withPassword( "password" );
            postgresContainer.start();
            var properties = Map.<String, Object>of(
                    "spring.r2dbc.host", postgresContainer.getHost(),
                    "spring.r2dbc.port", postgresContainer.getMappedPort( PostgreSQLContainer.POSTGRESQL_PORT ),
                    "spring.r2dbc.password", postgresContainer.getPassword(),
                    "spring.r2dbc.username", postgresContainer.getUsername(),
                    // Prevent any in memory db from replacing the data source
                    // See @AutoConfigureTestDatabase
                    "spring.test.database.replace", "NONE"
            );
            var propertySource = new MapPropertySource( "PostgresContainer Test Properties", properties );
            context.getEnvironment().getPropertySources().addFirst( propertySource );
        }

    }

}
