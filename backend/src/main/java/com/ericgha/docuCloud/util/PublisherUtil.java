package com.ericgha.docuCloud.util;

import reactor.core.publisher.Mono;

import java.util.function.Supplier;

public final class PublisherUtil {

    private PublisherUtil() throws IllegalAccessException {
        throw new IllegalAccessException( "Do not instantiate." );
    }

    static public <T> Mono<T> requireNonEmpty(Mono<T> mono, Supplier<RuntimeException> exceptionSupplier) throws RuntimeException {
        return mono.doOnSuccess( s -> {
            if (s == null) {
                throw exceptionSupplier.get();
            }
        } );
    }
}
