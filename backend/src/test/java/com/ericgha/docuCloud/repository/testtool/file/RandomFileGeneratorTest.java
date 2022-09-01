package com.ericgha.docuCloud.repository.testtool.file;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.random.RandomGenerator;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RandomFileGeneratorTest {

    private RandomFileGenerator randomFileGenerator;
    @Mock
    private RandomGenerator randomMock;

    static long NOT_RANDOM_LONG = 0x0807060504030201L;
    static byte[] NOT_RANDOM_BYTES = {1, 2, 3, 4, 5, 6, 7, 8};
    static int BUFFER_SIZE = 4096;

    @BeforeEach
    void before() {
        lenient().when( randomMock.nextLong() ).thenReturn( NOT_RANDOM_LONG );
        when( randomMock.nextInt( anyInt(), anyInt() ) ).thenCallRealMethod();
        Mockito.lenient().doCallRealMethod()
                .when(randomMock).nextBytes( any(byte[].class) );
        randomFileGenerator = RandomFileGenerator.builder()
                .algorithm( "SHA-1" ).bufferSizeB( BUFFER_SIZE )
                .random( randomMock ).build();
    }

    @DisplayName("Generate calculates the expected Checksum")
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 7, 8, 9, 10, 15, 16, 4095, 4096, 4097})
    void generateCalculatesCorrectHashCode(int sizeB) throws NoSuchAlgorithmException {
        randomFileGenerator.setMinMaxSizeB( sizeB, sizeB + 1 );
        var found = randomFileGenerator.generate()
                .fileDto().getChecksum();
        var digester = MessageDigest.getInstance( randomFileGenerator.getAlgorithm() );
        IntStream.range( 0, sizeB ).forEachOrdered( i -> digester.update( NOT_RANDOM_BYTES[i % 8] ) );
        var expected = Base64.getEncoder().encodeToString( digester.digest() );
        assertEquals( expected, found );
    }

    @ParameterizedTest
    @DisplayName("Generate returns the expected bytes")
    @ValueSource(ints = {0, 1, 7, 8, 9, 10, 15, 16, 4095, 4096, 4097})
    void generateReturnsExpectedData(int sizeB) {
        randomFileGenerator.setMinMaxSizeB( sizeB, sizeB + 1 );
        var found = randomFileGenerator.generate().data();
        List<ByteBuffer> expected = new ArrayList<>();
        while (sizeB > 0) {
            int numBytes = Math.min( BUFFER_SIZE, sizeB );
            sizeB -= BUFFER_SIZE;
            byte[] bytes = new byte[numBytes];
            IntStream.range( 0, numBytes ).forEach( i -> bytes[i] = NOT_RANDOM_BYTES[i % 8] );
            expected.add( ByteBuffer.wrap( bytes ) );
        }
        StepVerifier.create( found )
                .expectNextSequence( expected ).
                verifyComplete();
    }

    @Test
    @DisplayName( "generate produces a flux that can be subscribed to multiple times" )
    void generateCreatesFluxThatCanBeShared() {
        randomFileGenerator.setMinMaxSizeB( 8, 9 );
        var found = randomFileGenerator.generate().data();
        var expected = Long.reverseBytes( NOT_RANDOM_LONG );
        StepVerifier.create( found.map( ByteBuffer::getLong ) ).expectNext( expected )
                .verifyComplete();
        StepVerifier.create( found.map( ByteBuffer::getLong ) ).expectNext( expected )
                .verifyComplete();
    }

    @Test
    @DisplayName( "generate populates fileDto with correct size" )
    void generateReturnsCorrectSize() {
        final int expectedSize = 8;
        randomFileGenerator.setMinMaxSizeB( expectedSize, expectedSize + 1 );
        Long found = randomFileGenerator.generate()
                .fileDto().getSize();
        assertEquals(expectedSize, found);
    }
}
