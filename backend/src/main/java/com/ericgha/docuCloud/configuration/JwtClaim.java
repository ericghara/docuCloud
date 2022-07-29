package com.ericgha.docuCloud.configuration;

public enum JwtClaim {

    USER_ID("sub"),
    EMAIL("email"),
    EMAIL_VERIFIED("email_verified"),
    FULL_NAME("name"),
    USERNAME("preferred_username"),
    FIRST_NAME("given_name"),
    LAST_NAME("family_name"),
    REALM("azp"),
    SCOPE("scope");

    final String key;

    JwtClaim(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }
}
