package com.ericgha.docuCloud.converter;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
@NoArgsConstructor
public class LtreeToString implements Converter<Ltree, String> {

    @Override
    public String convert(@NonNull Ltree source) {
        return source.toString();
    }

    @Override
    public <U> Converter<Ltree, U> andThen(Converter<? super String, ? extends U> after) {
        return Converter.super.andThen( after );
    }
}
