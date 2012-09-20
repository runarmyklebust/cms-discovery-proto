package com.enonic.elasticsearch.discovery.jgroups;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryException;
import org.elasticsearch.discovery.InitialStateDiscoveryListener;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.transport.TransportService;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;


public class JGroupsDiscovery
    //extends AbstractLifecycleComponent<Discovery>
    implements Discovery, Receiver
{
    protected final ESLogger logger;

    private final Channel channel;

    private final ClusterName clusterName;

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final NetworkService networkService;

    private DiscoveryNode localNode;

    private Logger LOG = Logger.getLogger( JGroupsDiscovery.class.getName() );

    private volatile boolean addressSet = false;

    private volatile boolean firstMaster = false;

    private final AtomicBoolean initialStateSent = new AtomicBoolean();

    private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners =
        new CopyOnWriteArrayList<InitialStateDiscoveryListener>();

    private Settings settings;

    protected Settings componentSettings;

    @Inject
    public JGroupsDiscovery( Settings settings, Environment environment, ClusterName clusterName, TransportService transportService,
                             ClusterService clusterService, NetworkService networkService )
    {
        this.logger = Loggers.getLogger( getClass(), settings );

        this.settings = settings;
        this.componentSettings = settings.getComponentSettings( getClass() );

        this.clusterName = clusterName;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.networkService = networkService;

        String config = componentSettings.get( "config", "udp" );
        String actualConfig = config;
        if ( !config.endsWith( ".xml" ) )
        {
            actualConfig = "jgroups/" + config + ".xml";
        }

        URL configUrl = environment.resolveConfig( actualConfig );
        LOG.info( "Using configuration [{}]" + configUrl );

        Map<String, String> sysPropsSet = Maps.newHashMap();
        try
        {
            // prepare system properties to configure jgroups based on the settings
            for ( Map.Entry<String, String> entry : settings.getAsMap().entrySet() )
            {
                if ( entry.getKey().startsWith( "discovery.jgroups" ) )
                {
                    String jgroupsKey = entry.getKey().substring( "discovery.".length() );
                    if ( System.getProperty( jgroupsKey ) == null )
                    {
                        sysPropsSet.put( jgroupsKey, entry.getValue() );
                        System.setProperty( jgroupsKey, entry.getValue() );
                    }
                }
            }

            if ( System.getProperty( "jgroups.bind_addr" ) == null )
            {
                // automatically set the bind address based on ElasticSearch default bindings...
                try
                {
                    InetAddress bindAddress = networkService.resolveBindHostAddress( null, NetworkService.LOCAL );
                    if ( ( bindAddress instanceof Inet4Address && NetworkUtils.isIPv4() ) ||
                        ( bindAddress instanceof Inet6Address && !NetworkUtils.isIPv4() ) )
                    {
                        sysPropsSet.put( "jgroups.bind_addr", bindAddress.getHostAddress() );
                        System.setProperty( "jgroups.bind_addr", bindAddress.getHostAddress() );
                    }
                }
                catch ( IOException e )
                {
                    // ignore this
                }
            }

            channel = new JChannel( configUrl );
        }
        catch ( ChannelException e )
        {
            throw new DiscoveryException( "Failed to create jgroups channel with config [" + configUrl + "]", e );
        }
        finally
        {
            for ( String keyToRemove : sysPropsSet.keySet() )
            {
                System.getProperties().remove( keyToRemove );
            }
        }

    }

    @Override
    public void addListener( InitialStateDiscoveryListener listener )
    {
        initialStateListeners.add( listener );
    }

    @Override
    public void removeListener( InitialStateDiscoveryListener listener )
    {
        initialStateListeners.remove( listener );
    }

    protected void doStart()
        throws ElasticSearchException
    {
        try
        {
            channel.connect( clusterName.value() );
            channel.setReceiver( this );
            LOG.info( "Connected to cluster [{}], address [{}]" + channel.getClusterName() + channel.getAddress().toString() );
            this.localNode = new DiscoveryNode( settings.get( "name" ), channel.getAddress().toString(),
                                                transportService.boundAddress().publishAddress(), buildCommonNodesAttributes( settings ) );

            if ( isMaster() )
            {
                firstMaster = true;
                clusterService.submitStateUpdateTask( "jgroups-disco-initial_connect(master)", new ProcessedClusterStateUpdateTask()
                {
                    @Override
                    public ClusterState execute( ClusterState currentState )
                    {
                        DiscoveryNodes.Builder builder =
                            new DiscoveryNodes.Builder().localNodeId( localNode.id() ).masterNodeId( localNode.id() )
                                // put our local node
                                .put( localNode );
                        return newClusterStateBuilder().state( currentState ).nodes( builder ).build();
                    }

                    @Override
                    public void clusterStateProcessed( ClusterState clusterState )
                    {
                        sendInitialStateEventIfNeeded();
                    }
                } );
                addressSet = true;
            }
            else
            {
                clusterService.submitStateUpdateTask( "jgroups-disco-initialconnect", new ClusterStateUpdateTask()
                {
                    @Override
                    public ClusterState execute( ClusterState currentState )
                    {
                        DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder().localNodeId( localNode.id() ).put( localNode );
                        return newClusterStateBuilder().state( currentState ).nodes( builder ).build();
                    }
                } );
                try
                {
                    channel.send( new Message( channel.getView().getCreator(), channel.getAddress(), nodeMessagePayload() ) );
                    addressSet = true;
                    LOG.info( "Sent (initial) node information to master [{}], node [{}]" + channel.getView().getCreator() + localNode );
                }
                catch ( Exception e )
                {
                    LOG.warning( "Can't send address to master [" + channel.getView().getCreator() + "] will try again later..." );
                }
            }
        }
        catch ( ChannelException e )
        {
            throw new DiscoveryException( "Can't connect to group [" + clusterName + "]", e );
        }

    }

    public String nodeDescription()
    {
        return channel.getClusterName() + "/" + channel.getAddress();
    }

    public boolean firstMaster()
    {
        return firstMaster;
    }

    @Override
    public void publish( ClusterState clusterState )
    {
        if ( !isMaster() )
        {
            throw new ElasticSearchIllegalStateException( "Shouldn't publish state when not master" );
        }
        try
        {
            channel.send( new Message( null, null, ClusterState.Builder.toBytes( clusterState ) ) );
        }
        catch ( Exception e )
        {
            LOG.severe( "Failed to send cluster state to nodes" );
        }
    }


    @Override
    public DiscoveryNode localNode()
    {
        return null;
    }


    @Override
    public void setNodeService( @Nullable final NodeService nodeService )
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


    private boolean isMaster()
    {
        return channel.getAddress().equals( channel.getView().getCreator() );
    }

    public static Map<String, String> buildCommonNodesAttributes( Settings settings )
    {
        Map<String, String> attributes = Maps.newHashMap( settings.getByPrefix( "node." ).getAsMap() );
        if ( attributes.containsKey( "client" ) )
        {
            if ( attributes.get( "client" ).equals( "false" ) )
            {
                attributes.remove( "client" ); // this is the default
            }
            else
            {
                // if we are client node, don't store data ...
                attributes.put( "data", "false" );
            }
        }
        if ( attributes.containsKey( "data" ) )
        {
            if ( attributes.get( "data" ).equals( "true" ) )
            {
                attributes.remove( "data" );
            }
        }
        return attributes;
    }

    private byte[] nodeMessagePayload()
        throws IOException
    {
        CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();

        try
        {
            BytesStreamOutput os = cachedEntry.bytes();
            localNode.writeTo( os );
            return os.bytes().copyBytesArray().toBytes();
        }
        finally
        {
            CachedStreamOutput.pushEntry( cachedEntry );
        }

        //   BytesStreamOutput os = BytesStreamOutput.Cached.cached();
        //   localNode.writeTo( os );
        //   return os.bytes().copyBytesArray().toBytes();
    }


    private void sendInitialStateEventIfNeeded()
    {
        if ( initialStateSent.compareAndSet( false, true ) )
        {
            for ( InitialStateDiscoveryListener listener : initialStateListeners )
            {
                listener.initialStateProcessed();
            }
        }
    }

    @Override
    public void receive( Message msg )
    {
        if ( msg.getSrc().equals( channel.getAddress() ) )
        {
            return; // my own message, ignore.
        }

        // message from the master, the cluster state has changed.
        if ( msg.getSrc().equals( channel.getView().getCreator() ) )
        {
            try
            {
                byte[] buffer = msg.getBuffer();
                // RMY: final ClusterState clusterState = ClusterState.Builder.fromBytes( buffer, settings, localNode );
                final ClusterState clusterState = ClusterState.Builder.fromBytes( buffer, localNode );
                // ignore cluster state messages that do not include "me", not in the game yet...
                if ( clusterState.nodes().localNode() != null )
                {
                    clusterService.submitStateUpdateTask( "jgroups-disco-receive(from master)", new ProcessedClusterStateUpdateTask()
                    {
                        @Override
                        public ClusterState execute( ClusterState currentState )
                        {
                            return clusterState;
                        }

                        @Override
                        public void clusterStateProcessed( ClusterState clusterState )
                        {
                            sendInitialStateEventIfNeeded();
                        }
                    } );
                }
            }
            catch ( Exception e )
            {
                logger.info( "Received corrupted cluster state.", e );
            }

            return;
        }

        // direct message from a member indicating it has joined the jgroups cluster and provides us its node information
        if ( isMaster() )
        {
            try
            {
                // RMY: BytesStreamInput is = new BytesStreamInput( msg.getBuffer(), true );
                BytesStreamInput is = new BytesStreamInput( msg.getBuffer(), true );
                final DiscoveryNode newNode = DiscoveryNode.readNode( is );
                is.close();

                logger.debug( "Received node information from [{}], node [{}]", msg.getSrc(), newNode );

                if ( !transportService.addressSupported( newNode.address().getClass() ) )
                {
                    // TODO, what should we do now? Maybe inform that node that its crap?
                    logger.warn(
                        "Received a wrong address type from [" + msg.getSrc() + "], ignoring... (received_address[" + newNode.address() +
                            ")" );
                }
                else
                {
                    clusterService.submitStateUpdateTask( "jgroups-disco-receive(from node[" + newNode + "])", new ClusterStateUpdateTask()
                    {
                        @Override
                        public ClusterState execute( ClusterState currentState )
                        {
                            if ( currentState.nodes().nodeExists( newNode.id() ) )
                            {
                                // no change, the node already exists in the cluster
                                logger.warn( "Received an address [{}] for an existing node [{}]", newNode.address(), newNode );
                                return currentState;
                            }
                            return newClusterStateBuilder().state( currentState ).nodes( currentState.nodes().newNode( newNode ) ).build();
                        }
                    } );
                }
            }
            catch ( Exception e )
            {
                logger.warn(
                    "Can't read address from cluster member [" + msg.getSrc() + "] message [" + msg.getClass().getName() + "/" + msg + "]",
                    e );
            }

            return;
        }

        logger.error( "A message between two members that neither of them is the master is not allowed." );
    }

    @Override
    public void viewAccepted( final View newView )
    {
        if ( !addressSet )
        {
            try
            {
                channel.send( new Message( newView.getCreator(), channel.getAddress(), nodeMessagePayload() ) );
                logger.debug( "Sent (view) node information to master [{}], node [{}]", newView.getCreator(), localNode );
                addressSet = true;
            }
            catch ( Exception e )
            {
                logger.warn( "Can't send address to master [" + newView.getCreator() + "] will try again later...", e );
            }
        }
        // I am the master
        if ( channel.getAddress().equals( newView.getCreator() ) )
        {
            final Set<String> newMembers = Sets.newHashSet();
            for ( Address address : newView.getMembers() )
            {
                newMembers.add( address.toString() );
            }

            clusterService.submitStateUpdateTask( "jgroups-disco-view", new ClusterStateUpdateTask()
            {
                @Override
                public ClusterState execute( ClusterState currentState )
                {
                    DiscoveryNodes newNodes = currentState.nodes().removeDeadMembers( newMembers, newView.getCreator().toString() );
                    DiscoveryNodes.Delta delta = newNodes.delta( currentState.nodes() );
                    if ( delta.added() )
                    {
                        logger.warn( "No new nodes should be created when a new discovery view is accepted" );
                    }
                    // we want to send a new cluster state any how on view change (that's why its commented)
                    // for cases where we have client node joining (and it needs the cluster state)
//                    if (!delta.removed()) {
//                        // no nodes were removed, return the current state
//                        return currentState;
//                    }
                    return newClusterStateBuilder().state( currentState ).nodes( newNodes ).build();
                }
            } );
        }
        else
        {
            // check whether I have been removed due to temporary disconnect
            final String me = channel.getAddress().toString();
            boolean foundMe = false;
            for ( DiscoveryNode node : clusterService.state().nodes() )
            {
                if ( node.id().equals( me ) )
                {
                    foundMe = true;
                    break;
                }
            }

            if ( !foundMe )
            {
                logger.warn( "Disconnected from cluster, resending to master [{}], node [{}]", newView.getCreator(), localNode );
                try
                {
                    channel.send( new Message( newView.getCreator(), channel.getAddress(), nodeMessagePayload() ) );
                    addressSet = true;
                }
                catch ( Exception e )
                {
                    addressSet = false;
                    logger.warn( "Can't send address to master [" + newView.getCreator() + "] will try again later...", e );
                }
            }
        }
    }


    @Override
    public void suspect( final Address address )
    {

    }

    @Override
    public void block()
    {
        LOG.warning( "Blocked..." );
    }


    @Override
    public byte[] getState()
    {
        return new byte[0];
    }

    @Override
    public void setState( final byte[] bytes )
    {

    }


}
