package com.ericgha.docuCloud.configuration;

import com.ericgha.docuCloud.configuration.properties.R2dbcPropertiesKey;
import com.ericgha.docuCloud.service.AppEnvPropertiesService;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jooq.DefaultConfigurationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.lang.NonNullApi;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableTransactionManagement
@EnableR2dbcRepositories
@RequiredArgsConstructor
public class JooqConfiguration extends AbstractR2dbcConfiguration {
    private final AppEnvPropertiesService propertiesService;

    @Bean
    public DefaultConfigurationCustomizer configurationCustomizer() {
        return c -> c.settings()
                .withRenderQuotedNames( RenderQuotedNames.EXPLICIT_DEFAULT_UNQUOTED );
    }

    @Bean
    public DSLContext jooqDslContext(@Autowired ConnectionFactory cfi) {
        return DSL.using( new TransactionAwareConnectionFactoryProxy(cfi), SQLDialect.POSTGRES, new Settings().withRenderFormatted(true)
                        .withBindOffsetDateTimeType( true )
                        .withBindOffsetTimeType( true ) )
                .dsl();
    }

    @Override
    @Bean
    public ConnectionFactory connectionFactory() {
        return new PostgresqlConnectionFactory( PostgresqlConnectionConfiguration.builder()
                .username( propertiesService.get( R2dbcPropertiesKey.USERNAME ) )
                .password( propertiesService.get(R2dbcPropertiesKey.PASSWORD) )
                .host( propertiesService.get(R2dbcPropertiesKey.HOST) )
                .port( Integer.parseInt(propertiesService.get( R2dbcPropertiesKey.PORT ) ) )
                .database( propertiesService.get( R2dbcPropertiesKey.NAME ) ).build() );
    }
}
