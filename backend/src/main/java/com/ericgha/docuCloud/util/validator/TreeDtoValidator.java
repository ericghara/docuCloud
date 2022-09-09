package com.ericgha.docuCloud.util.validator;

import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.exceptions.IllegalObjectTypeException;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.service.DocumentService;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Slf4j
public class TreeDtoValidator {

    private TreeDtoValidator() throws IllegalAccessException {
        throw new IllegalAccessException("Do not instantiate");
    }

    public static ObjectType mustBeObjectType(TreeDto treeDto, ObjectType expected) throws IllegalObjectTypeException {
        final ObjectType found = treeDto.getObjectType();
        final String msgTemplate = "Expected ObjectType: %s but Found: %s";
        if (found == null) {
            log.debug( "Invalid treeDto: {}", treeDto );
            throw new IllegalObjectTypeException( String.format( msgTemplate, found, "null" ) );
        }
        if (expected != found) {
            log.debug( "Invalid treeDto: {}", treeDto );
            throw new IllegalObjectTypeException(
                    String.format( msgTemplate, expected, treeDto.getObjectType() ) );
        }
        return found;
    }

    public static ObjectType mustBeOneOfObjectTypes(TreeDto treeDto, ObjectType... objectTypes) throws IllegalObjectTypeException {
        String messageTemplate = "Allowed objectType(s): %s but Found: %s";
        ObjectType found;
        try {
            found = getOrThrow( treeDto.getObjectType(), "objectType" );
        } catch (NullPointerException e) {
            throw new IllegalObjectTypeException( String.format( messageTemplate, Arrays.toString( objectTypes ), "null" ) );
        }
        if (!List.of( objectTypes ).contains( found )) {
            throw new IllegalObjectTypeException( String.format( messageTemplate, Arrays.toString( objectTypes ), found ) );
        }
        return found;
    }

    public static <T> T getOrThrow(T object, String field) throws NullPointerException {
        Objects.requireNonNull( object,
                String.format( "received a null %s.", field ) );
        return object;
    }
}
