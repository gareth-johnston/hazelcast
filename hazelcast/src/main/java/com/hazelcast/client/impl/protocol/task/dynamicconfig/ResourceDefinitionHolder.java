package com.hazelcast.client.impl.protocol.task.dynamicconfig;

public class ResourceDefinitionHolder {

    private final String id;
    private final int resourceType;
    private final byte[] payload;

    public ResourceDefinitionHolder(String id, int resourceType, byte[] payload) {
        this.id = id;
        this.resourceType = resourceType;
        this.payload = payload;
    }

    public String getId() {
        return id;
    }

    public int getResourceType() {
        return resourceType;
    }

    public byte[] getPayload() {
        return payload;
    }
}
