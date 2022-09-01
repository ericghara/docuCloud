package com.ericgha.docuCloud.service;

import io.r2dbc.spi.ConnectionFactory;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.reactivestreams.Publisher;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

@RequiredArgsConstructor
public class JooqTransaction {

    private final ConnectionFactory cfi;
    private final SQLDialect sqlDialect;
    private final Settings dslSettings;

    public Mono<DSLContext> get() {
        // publish a transaction aware connection
        // and wrap it in a DSLContext
        return ConnectionFactoryUtils.getConnection( cfi )
                .map(c -> DSL.using(c, sqlDialect, dslSettings) );
    }

    public <T> Mono<T> transact(@NonNull Function<DSLContext, Publisher<T>> monoFunction) {
        return this.get().flatMap( trxDsl -> Mono.from(monoFunction.apply(trxDsl) ) );
    }

    public <T> Flux<T> transactMany(@NonNull Function<DSLContext, Publisher<T>> fluxFunction) {
        return this.get().flatMapMany( trxDsl -> Flux.from(fluxFunction.apply(trxDsl) ) );
    }


}
