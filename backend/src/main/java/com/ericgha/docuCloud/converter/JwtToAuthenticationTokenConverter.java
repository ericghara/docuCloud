package com.ericgha.docuCloud.converter;

import com.ericgha.docuCloud.configuration.JwtClaim;
import com.ericgha.docuCloud.dto.CloudUser;
import lombok.RequiredArgsConstructor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
public class JwtToAuthenticationTokenConverter implements Converter<Jwt, Mono<AbstractAuthenticationToken>> {
    private static final String ROLE_PREFIX = "ROLE_";
    private final JwtToCloudUserConverter jwtCloudUserConverter;

    @Override
    public Mono<AbstractAuthenticationToken> convert(Jwt source) {
        CloudUser CloudUser= jwtCloudUserConverter.convert(source);
        Collection<GrantedAuthority> authorities = extractAuthorities( source, CloudUser.getUserId() );
        return Mono.just(new UsernamePasswordAuthenticationToken( jwtCloudUserConverter.convert(source),
                "n/a", authorities) );
    }

    @Override
    public <U> Converter<Jwt, U> andThen(Converter<? super Mono<AbstractAuthenticationToken>, ? extends U> after) {
        return Converter.super.andThen( after );
    }

    private List<GrantedAuthority> extractAuthorities(Jwt jwt, String userId) {
        Stream<String> scopes = getScopes( jwt ).stream()
                .map( s -> ROLE_PREFIX + s.toUpperCase() );
        Stream<String> selfAuthority = Stream.of(userId);
        return Stream.concat(scopes, selfAuthority)
                .map( SimpleGrantedAuthority::new )
                .map( a -> (GrantedAuthority) a )
                .toList();
    }

    private Collection<String> getScopes(Jwt jwt) {
        String[] scopes = jwt.getClaims().get( JwtClaim.SCOPE.key() ).toString().split("\s");
        return Set.of(scopes);
    }
}
