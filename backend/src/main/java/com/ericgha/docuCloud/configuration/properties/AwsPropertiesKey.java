package com.ericgha.docuCloud.configuration.properties;

import com.ericgha.docuCloud.service.PropertyKey;

public enum AwsPropertiesKey implements PropertyKey {

    ACCESS_KEY_ID( "access-key-id" ),
    SECRET_ACCESS_KEY( "secret-access-key" ),
    S3_ENDPOINT( "s3-endpoint" ),
    S3_BUCKET( "s3-bucket" ),
    S3_ENDPOINT_OVERRRIDE("s3-endpoint-override"),
    REGION("region");

    public static final String PREFIX = "app.s3.";
    private final String key;

    AwsPropertiesKey(String key) {
        this.key = key;
    }

    public String get() {
        return PREFIX + key;
    }

}
