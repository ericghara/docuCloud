package com.ericgha.docuCloud.converter;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
@NoArgsConstructor
public class StringToLtree implements Converter<String, Ltree> {

    @Override
    public Ltree convert(@NonNull String source) {
        return Ltree.valueOf(source);
    }

    @Override
    public <U> Converter<String, U> andThen(Converter<? super Ltree, ? extends U> after) {
        return Converter.super.andThen( after );
    }
}
