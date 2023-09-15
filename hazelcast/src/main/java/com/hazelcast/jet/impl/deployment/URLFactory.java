package com.hazelcast.jet.impl.deployment;

import java.net.MalformedURLException;
import java.net.URL;

@FunctionalInterface
public interface URLFactory {

    URL create(String resource) throws MalformedURLException;
}
