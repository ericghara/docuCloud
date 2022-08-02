package com.ericgha.docuCloud.converter;

import com.ericgha.docuCloud.configuration.JwtClaim;
import com.ericgha.docuCloud.dto.CloudUser;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
@NoArgsConstructor
/*
 * Creates CloudUser from client_scopes encoded in jwt
 */
public class JwtToCloudUserConverter implements Converter<Jwt, CloudUser> {

    @Override
    public CloudUser convert(Jwt source) {
        return CloudUser.builder()
                .userId( UUID.fromString(source.getClaimAsString( JwtClaim.USER_ID.key() ) ) )
                .username( source.getClaimAsString( JwtClaim.USERNAME.key() ) )
                .email( source.getClaimAsString( JwtClaim.EMAIL.key() ) )
                .emailVerified( source.getClaimAsBoolean( JwtClaim.EMAIL_VERIFIED.key() ) )
                .fullName( source.getClaimAsString( JwtClaim.FULL_NAME.key() ) )
                .firstName( source.getClaimAsString( JwtClaim.FIRST_NAME.key() ) )
                .lastName( source.getClaimAsString( JwtClaim.LAST_NAME.key() ) )
                .realm( source.getClaimAsString( JwtClaim.REALM.key() ) )
                .build();
    }

    @Override
    public <U> Converter<Jwt, U> andThen(Converter<? super CloudUser, ? extends U> after) {
        return Converter.super.andThen( after );
    }
}
