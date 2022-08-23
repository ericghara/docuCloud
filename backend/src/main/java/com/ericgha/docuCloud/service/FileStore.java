package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.Bucket;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.UUID;
import java.util.zip.Checksum;

/**
 * Interface for persisting file data.
 * Methods that return {@code Mono<Void>} throw {@code RuntimeException}s on error.
 */
public interface FileStore {

    Mono<Void> isReady() throws RuntimeException;

    /**
     *
     * @return true if bucket was created, false if it exists and is accessible
     * @throws RuntimeException if bucket cannot be created or exists and is not accessible
     */
    Mono<Boolean> createBucketIfNotExists() throws RuntimeException;

    Mono<Void> putFile(Flux<ByteBuffer> data, FileDto filDto, CloudUser cloudUser) throws RuntimeException;

    Flux<ByteBuffer> getFile(FileDto fileDto, CloudUser cloudUser);

    Mono<Void> deleteFile(FileDto fileDto, CloudUser cloudUser) throws RuntimeException;

    Mono<Void> deleteFiles(Flux<FileDto> fileDtos, CloudUser cloudUser) throws RuntimeException;

}
