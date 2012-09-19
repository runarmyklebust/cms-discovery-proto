package com.enonic.elasticsearch.discovery;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.node.Node;

public class ElasticsearchInstance
{

    public static final String LINE_SEPARATOR = System.getProperty( "line.separator" );

    private Node node;

    private NodeFactory nodeFactory = new NodeFactory();

    public void start()
        throws Exception
    {
        this.node = nodeFactory.createNode();
        this.node.start();
    }

    public void stop()
    {
        this.node.stop();
    }

    public String getStatus()
    {
        StringBuilder builder = new StringBuilder();

        getNodeStatus( builder );

        return builder.toString();
    }

    private void getNodeStatus( StringBuilder builder )
    {
        NodesStatsRequest request = new NodesStatsRequest();

        final NodesStatsResponse nodeStats = this.node.client().admin().cluster().nodesStats( request ).actionGet();

        builder.append( "Number of nodes: " + nodeStats.nodes().length + LINE_SEPARATOR );

        for ( NodeStats nodeStat : nodeStats.getNodes() )
        {
            builder.append( nodeStat.getHostname() + LINE_SEPARATOR );
        }

    }

    private void getNodeSettings( final StringBuilder builder )
    {
        final ImmutableMap<String, String> nodeSettingsMap = this.node.settings().getAsMap();

        for ( String setting : nodeSettingsMap.keySet() )
        {
            builder.append( setting + " : " + nodeSettingsMap.get( setting ) + LINE_SEPARATOR );
        }
    }


}
