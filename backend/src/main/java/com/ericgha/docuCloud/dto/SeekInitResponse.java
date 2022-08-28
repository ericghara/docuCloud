package com.ericgha.docuCloud.dto;

import lombok.NonNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public record SeekInitResponse(@NonNull Flux<FileDto> firstPage, @NonNull Mono<Long> numVersions) {

}
