package com.ericgha.docuCloud.configuration;

import com.ericgha.docuCloud.service.TransactionAwareDsl;
import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jooq.DefaultConfigurationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableTransactionManagement
@AllArgsConstructor
public class JooqConfiguration {

    private final ConnectionFactory cfi;
    private static final Settings DSL_SETTINGS = new Settings().withRenderFormatted( true )
            .withBindOffsetDateTimeType( true )
            .withBindOffsetTimeType( true );
    private static final SQLDialect DSL_DIALECT = SQLDialect.POSTGRES;

    @Bean
    public DefaultConfigurationCustomizer configurationCustomizer() {
        return c -> c.settings()
                .withRenderQuotedNames( RenderQuotedNames.EXPLICIT_DEFAULT_UNQUOTED );
    }

    // This may be removed, or limited in scope.  It
    // only should be used for testing as its connections
    // are not transaction aware.
    @Bean
    public DSLContext jooqDslContext() {
        return DSL.using( cfi, DSL_DIALECT, DSL_SETTINGS )
                .dsl();
    }

    @Bean
    public TransactionAwareDsl dslPublisher(@Autowired DSLContext dsl) {
        return new TransactionAwareDsl(cfi, DSL_DIALECT, DSL_SETTINGS);
    }

//    @Bean
    // todo delete me
//    public DatabaseClient databaseClient() {
//        // convert to transaciton aware???
//        return DatabaseClient.create( cfi );
//    }
}
