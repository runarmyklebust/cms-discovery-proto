package com.enonic.elasticsearch.discovery.jgroups;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.discovery.Discovery;

public class JGroupsDiscoveryModule
    extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind( Discovery.class ).to( JGroupsDiscovery.class ).asEagerSingleton();
    }

}
