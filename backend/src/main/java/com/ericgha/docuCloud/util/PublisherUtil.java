package com.ericgha.docuCloud.util;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;
import java.util.function.Supplier;

public final class PublisherUtil {

    private PublisherUtil() throws IllegalAccessException {
        throw new IllegalAccessException( "Do not instantiate." );
    }

    static public <T> Mono<T> requireNext(Mono<T> mono, Function<Throwable, RuntimeException> exceptionFunction) throws RuntimeException {
        return mono.switchIfEmpty( Mono.error( exceptionFunction.apply(null) ) )
                .onErrorMap( exceptionFunction );
    }

    static public <T> Flux<T> requireNonEmpty(Flux<T> flux, Function<Throwable, RuntimeException> exceptionFunction) throws RuntimeException {
        return flux.switchIfEmpty( Flux.error(exceptionFunction.apply(null) ) )
                .onErrorMap( exceptionFunction );
    }

    static public <T extends Number> Mono<T> requireNonZero(Mono<T> mono, Supplier<RuntimeException> exceptionSupplier ) {
        return mono.handle( (num, sink) -> {
            if (num.longValue() == 0L) {
                sink.error( exceptionSupplier.get() );
            } else {
                sink.next( num );
            }
        } );
    }

    static public <T extends Number> Flux<T> requireNonZero(Flux<T> flux, Supplier<RuntimeException> exceptionSupplier ) {
        return flux.handle( (num, sink) ->  {
            if (num.longValue() == 0L) {
                sink.error( exceptionSupplier.get() );
            }
            else {
                sink.next( num );
            }
        } );
    }
}
