package com.ericgha.docuCloud.configuration;

import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.springframework.boot.autoconfigure.jooq.DefaultConfigurationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@AllArgsConstructor
public class JooqConfiguration {

    private final ConnectionFactory cfi;

    @Bean
    public DefaultConfigurationCustomizer configurationCustomiser() {
        return c -> c.settings()
                .withRenderQuotedNames( RenderQuotedNames.EXPLICIT_DEFAULT_UNQUOTED );
    }

    @Bean
    public DSLContext jooqDslContext() {
        return DSL.using( cfi, SQLDialect.POSTGRES, new Settings().withRenderFormatted(true) ).
                dsl();
    }
}