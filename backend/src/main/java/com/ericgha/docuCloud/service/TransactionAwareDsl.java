package com.ericgha.docuCloud.service;

import io.r2dbc.spi.ConnectionFactory;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class TransactionAwareDsl {

    private final ConnectionFactory cfi;
    private final SQLDialect sqlDialect;
    private final Settings dslSettings;

    public Mono<DSLContext> get() {
        // publish a transaction aware connection
        // and wrap it in a DSLContext
        return ConnectionFactoryUtils.getConnection( cfi )
                .map(c -> DSL.using(c, sqlDialect, dslSettings) );
    }
}
