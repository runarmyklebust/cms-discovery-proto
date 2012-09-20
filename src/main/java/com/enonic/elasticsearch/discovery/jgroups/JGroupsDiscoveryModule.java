package com.enonic.elasticsearch.discovery.jgroups;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;

public class JGroupsDiscoveryModule
    extends AbstractModule
{
    private final Settings settings;

    public JGroupsDiscoveryModule( Settings settings )
    {
        this.settings = settings;
    }

    @Override
    protected void configure()
    {
        bind( Discovery.class ).to( JGroupsDiscovery.class ).asEagerSingleton();
    }

}
