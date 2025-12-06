package com.apacy.queryprocessor;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ServerConfig {
    
    @JsonProperty("algorithm")
    public String algorithm = "lock";

    public String getAlgorithm() { return algorithm; }
}