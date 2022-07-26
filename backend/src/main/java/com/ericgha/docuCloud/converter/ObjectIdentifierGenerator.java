package com.ericgha.docuCloud.converter;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.util.List;
import java.util.UUID;

/**
 * Generates filenames for FileStore.  Object names are generated by concatenating joining {@code ObjectId} and
 * {@code fileId} with a {@code .} delimiter (i.e. {ObjectId}.{FileId} ).  The length of each fileKey is 73 characters.
 */
public class ObjectIdentifierGenerator {

    private static final String FORMAT_TEMPLATE = "%1$s.%2$s";

    public static <T extends FileDto> ObjectIdentifier generate(T fileDto, CloudUser cloudUser) {
        return ObjectIdentifierGenerator.generate( fileDto.getFileId(), cloudUser );
    }

    public static List<ObjectIdentifier> generate(List<UUID> fileIds, CloudUser cloudUser) {
        return fileIds.stream()
                .map(fileId -> ObjectIdentifierGenerator.generate( fileId, cloudUser ) )
                .toList();
    }

    public static ObjectIdentifier generate(UUID fileId, CloudUser cloudUser) {
        var key = String.format(FORMAT_TEMPLATE, cloudUser.getUserId(), fileId );
        return ObjectIdentifier.builder()
                .key( key )
                .build();
    }
}
