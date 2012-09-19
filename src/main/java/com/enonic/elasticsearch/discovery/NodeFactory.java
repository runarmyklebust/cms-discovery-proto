package com.enonic.elasticsearch.discovery;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class NodeFactory
{

    private SettingsProvider settingsProvider = new SettingsProvider();

    public Node createNode()
        throws Exception
    {
        final Settings settings = settingsProvider.createNodeSettings();

        Node node = NodeBuilder.nodeBuilder().local( false ).client( false ).data( true ).settings( settings ).build();

        return node;
    }


}
