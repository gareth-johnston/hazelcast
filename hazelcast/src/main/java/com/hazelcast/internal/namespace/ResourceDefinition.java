package com.hazelcast.internal.namespace;

import com.hazelcast.jet.config.ResourceType;

// todo: decide on API: do we want to expose a generic {id, type} resource API or narrow down API (e.g. "addFile", "addClass" etc)
//  and use types internally?
public interface ResourceDefinition {

    /**
     * Returns the identifier of this resource. For example, for a {@code CLASS} type resource it can be the fully-qualified
     * name of the class.
     * @return the identifier of this resource.
     */
    String id();

    /**
     * @return the type of the resource.
     */
    ResourceType type();

    /**
     * @return the contents of the resource.
     * todo byte[] or input stream? if the latter, what is its lifecycle?
     */
    byte[] payload();
}
