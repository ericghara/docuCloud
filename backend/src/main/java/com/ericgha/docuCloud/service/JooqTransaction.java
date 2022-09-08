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


    public <T> Mono<T> transact(@NonNull Function<DSLContext, Publisher<T>> monoFunction) {
//        var txop = TransactionalOperator.create(transactionManager);
        return databaseClient.inConnection( conn -> {
            var trxDsl = DSL.using( conn );
            return Mono.from( monoFunction.apply( trxDsl ) )
                    .doOnCancel( () -> Mono.from( conn.commitTransaction() )
                            .subscribeOn( Schedulers.boundedElastic() )
                            .subscribe() )
                    .doOnError( e -> Mono.from( conn.rollbackTransaction() )
                            .subscribeOn( Schedulers.boundedElastic() )
                            .subscribe() );
//                    .as(txop::transactional);
        } );
    }


    public <T> Flux<T> transactMany(@NonNull Function<DSLContext, Publisher<T>> fluxFunction) {
//        var txop = TransactionalOperator.create(transactionManager);
        return databaseClient.inConnectionMany( conn -> {
            var trxDsl = DSL.using( conn, sqlDialect, dslSettings );
            return Flux.from( fluxFunction.apply( trxDsl ) )
                    .doOnCancel( () -> Mono.from( conn.commitTransaction() )
                            .subscribeOn( Schedulers.boundedElastic() )
                            .subscribe() )
                    .doOnError( e -> Mono.from( conn.rollbackTransaction() )
                            .subscribeOn( Schedulers.boundedElastic() )
                            .subscribe() );
//                    .as(txop::transactional);
        } );
    }
}
