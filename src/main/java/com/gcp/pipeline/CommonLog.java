package com.gcp.pipeline;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaFieldSchema.class)
public class CommonLog {
    int id;
    String name;
    String surname;
}