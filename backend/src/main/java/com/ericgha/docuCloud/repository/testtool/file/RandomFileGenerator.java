package com.ericgha.docuCloud.repository.testtool.file;

import com.ericgha.docuCloud.dto.FileDto;
import com.ongres.scram.common.bouncycastle.base64.Base64;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.random.RandomGenerator;

@Builder
public class RandomFileGenerator {

    public record FileDtoAndData(@NonNull FileDto fileDto, @NonNull Flux<ByteBuffer> data) {
    }

    private static final int DEFAULT_MIN_SIZE_B = 256;
    private static final int DEFAULT_MAX_SIZE_B = 1024;
    private static int DEFAULT_BUFFER_SIZE_B = 4096; // must be a multiple of 8
    private static long MAX_ALLOWED_SIZE_B = 0x1_000_000; // 16 MB
    private static final String DEFAULT_ALGORITHM = "SHA-1";

    @Getter
    private volatile int minSizeB;
    @Getter
    private volatile int maxSizeB;
    @Getter
    private final String algorithm;
    private final RandomGenerator random;
    @Getter
    private final int bufferSizeB;

    /**
     * @param minSizeB  inclusive
     * @param maxSizeB  exclusive
     * @param algorithm
     */
    private RandomFileGenerator(@Nullable Integer minSizeB, @Nullable Integer maxSizeB,
                               @Nullable String algorithm, @Nullable RandomGenerator random,
                               @Nullable Integer bufferSizeB)  {
        this.minSizeB = Objects.requireNonNullElse( minSizeB, DEFAULT_MIN_SIZE_B );
        this.maxSizeB = Objects.requireNonNullElse( maxSizeB, DEFAULT_MAX_SIZE_B );
        this.algorithm = Objects.requireNonNullElse( algorithm, DEFAULT_ALGORITHM );
        this.bufferSizeB = Objects.requireNonNullElse(bufferSizeB, DEFAULT_BUFFER_SIZE_B );
        this.random = Objects.requireNonNullElse( random, new SplittableRandom() );
    }

    /**
     * Construct with defaults
     */
    public RandomFileGenerator() {
        this( null, null, null, null, null );
    }

    public synchronized void setMinMaxSizeB(int minSizeB, int maxSizeB) {
        if (minSizeB < 0 || minSizeB > maxSizeB || maxSizeB > MAX_ALLOWED_SIZE_B) {
            throw new IllegalArgumentException( "minSize must be smaller than maxSize" );
        }
        this.minSizeB = minSizeB;
        this.maxSizeB = maxSizeB;
    }

    public FileDtoAndData generate() {
        int sizeB = random.nextInt( minSizeB, maxSizeB );
        // since we need to calculate checksum before
        // sending data, there's no (sane) way to do
        // this without generating all the bytes up front
        // and then digesting.
        byte[][] matrix = this.generateMatrix( sizeB );
        String checksum = this.hash(matrix);
        Flux<ByteBuffer> bufferFlux = Flux.fromArray(matrix)
                .mapNotNull(ByteBuffer::wrap );
        FileDto fileDto = FileDto.builder()
                .checksum( checksum )
                .size( (long) sizeB )
                .build();
        return new FileDtoAndData( fileDto, bufferFlux );
    }

    // x is each buffer block
    // y is byte # in block
    private byte[][] generateMatrix(int sizeB) {
        final int fullBlock = sizeB / DEFAULT_BUFFER_SIZE_B;
        final int lastBlockY = sizeB % DEFAULT_BUFFER_SIZE_B;
        final int blocks = lastBlockY > 0 ? fullBlock + 1 : fullBlock;
        byte[][] matrix = new byte[blocks][];
        for (int i = 0; i < fullBlock; i++) {
            matrix[i] = this.generateColumn( DEFAULT_BUFFER_SIZE_B );
        }
        if (lastBlockY > 0) {
            matrix[blocks - 1] = this.generateColumn(lastBlockY);
        }
        return matrix;
    }

    private byte[] generateColumn(int len) {
        final long MASK = 0x00000000000000FFL;
        final int BYTES_PER_LONG = 8;
        long byteSource = 0;
        int bytesWritten = BYTES_PER_LONG;
        byte[] column = new byte[len];
        for (int i = 0; i < len; i++) {
            if (bytesWritten >= 8) {
                byteSource = random.nextLong();
                bytesWritten = 0;
            }
            column[i] = (byte) ( byteSource & MASK );
            byteSource >>>= 8;
            bytesWritten++;
        }
        return column;
    }

    private String hash(byte[][] matrix) {
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance( this.algorithm );
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unrecognized hash algorithm " + this.algorithm, e);
        }
        for (byte[] column : matrix) {
            messageDigest.update( column );
        }
        byte[] digest = messageDigest.digest();
        return Base64.toBase64String(digest);
    }
}
