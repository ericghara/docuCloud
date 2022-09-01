package com.ericgha.docuCloud.util;

import reactor.core.publisher.Mono;

import java.util.function.Function;

public final class PublisherUtil {

    private PublisherUtil() throws IllegalAccessException {
        throw new IllegalAccessException( "Do not instantiate." );
    }

    static public <T> Mono<T> requireNext(Mono<T> mono, Function<Throwable, RuntimeException> exceptionFunction) throws RuntimeException {
        return mono.doOnSuccess( s -> {
            if (s == null) {
                throw exceptionFunction.apply(null);
            }
        } )
        .doOnError( e -> { throw exceptionFunction.apply(e); } );
    }
}
