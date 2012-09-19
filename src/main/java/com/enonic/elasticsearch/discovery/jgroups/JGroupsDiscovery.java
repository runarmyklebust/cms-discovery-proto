package com.enonic.elasticsearch.discovery.jgroups;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.InitialStateDiscoveryListener;
import org.elasticsearch.node.service.NodeService;

public class JGroupsDiscovery
    implements Discovery
{
    private final Settings settings;

    @Inject
    public JGroupsDiscovery( final Settings settings )
    {
        this.settings = settings;
    }

    @Override
    public DiscoveryNode localNode()
    {
        return null;
    }

    @Override
    public void addListener( final InitialStateDiscoveryListener listener )
    {

    }

    @Override
    public void removeListener( final InitialStateDiscoveryListener listener )
    {

    }

    @Override
    public String nodeDescription()
    {
        return null;
    }

    @Override
    public void setNodeService( @Nullable final NodeService nodeService )
    {

    }

    @Override
    public void publish( final ClusterState clusterState )
    {

    }

    @Override
    public Lifecycle.State lifecycleState()
    {
        return null;
    }

    @Override
    public void addLifecycleListener( final LifecycleListener listener )
    {

    }

    @Override
    public void removeLifecycleListener( final LifecycleListener listener )
    {

    }

    @Override
    public Discovery start()
        throws ElasticSearchException
    {
        return null;
    }

    @Override
    public Discovery stop()
        throws ElasticSearchException
    {
        return null;
    }

    @Override
    public void close()
        throws ElasticSearchException
    {

    }
}
