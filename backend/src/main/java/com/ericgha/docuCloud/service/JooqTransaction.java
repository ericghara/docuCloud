package com.ericgha.docuCloud.service;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.reactivestreams.Publisher;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.function.Function;

@RequiredArgsConstructor
public class JooqTransaction {

    private final DatabaseClient databaseClient;

    private final ReactiveTransactionManager transactionManager;

    private final SQLDialect sqlDialect;
    private final Settings dslSettings;

    public Mono<DSLContext> get() {
        // publish a transaction aware connection
        // and wrap it in a DSLContext
        return ConnectionFactoryUtils.getConnection( databaseClient.getConnectionFactory() )
                .map( c -> DSL.using( c, sqlDialect, dslSettings ) );
    }

    /**
     * Wrap the provided mono in a transaction managed by
     * {@link org.springframework.transaction.ReactiveTransactionManager}
     * @param mono stream to wrap
     * @return data stream
     * @param <T> data type
     */
    public <T> Mono<T> inTransaction(Mono<T> mono) {
        return TransactionalOperator.create( transactionManager )
                .transactional( mono );
    }

    /**
     * Wrap the provided flux in a transaction managed by
     * {@link org.springframework.transaction.ReactiveTransactionManager}
     * @param flux stream to wrap
     * @return data stream
     * @param <T> data type
     */
    public <T> Flux<T> inTransaction(Flux<T> flux) {
        return TransactionalOperator.create( transactionManager )
                .transactional( flux );
    }


    /**
     * Provides a transaction aware connection to a query.  Does not itself make the query
     * transactional.  Currently, {@link JooqTransaction#inTransaction} is the way to do that. Spring declarative
     * transactions are buggy (unknown reason) with this approach and should be avoided.
     * @param monoFunction with DSLContext as an argument returning a mono
     * @return data stream
     * @param <T> Data type of mono
     */
    public <T> Mono<T> withConnection(@NonNull Function<DSLContext, Publisher<T>> monoFunction) {
        return databaseClient.inConnection( conn -> {
            var trxDsl = DSL.using( conn );
            return Mono.from( monoFunction.apply( trxDsl ) );
        } );
    }


    /**
     * Provides a transaction aware connection to a query.  Does not itself make the query
     * transactional.  Currently, {@link JooqTransaction#inTransaction} is the way to do that. Spring declarative
     * transactions are buggy (unknown reason) with this approach and should be avoided.
     * @param fluxFunction with DSLContext as an argument returning a flux
     * @return data stream
     * @param <T> Data type of flux
     */
    public <T> Flux<T> withConnectionMany(@NonNull Function<DSLContext, Publisher<T>> fluxFunction) {
        return databaseClient.inConnectionMany( conn -> {
            var trxDsl = DSL.using( conn, sqlDialect, dslSettings );
            return Flux.from( fluxFunction.apply( trxDsl ) );
        } );
    }
}
