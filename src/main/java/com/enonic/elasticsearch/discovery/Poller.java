package com.enonic.elasticsearch.discovery;

public class Poller
{
    private final ElasticsearchInstance server;

    public Poller( final ElasticsearchInstance server )
    {
        this.server = server;
    }

    public void poll( int numberOfPolls )
        throws Exception
    {
        int i = 0;

        while ( numberOfPolls == 0 || i++ < numberOfPolls )
        {
            // System.out.println( "Status: " + this.server.getStatus() );
            Thread.sleep( 2000 );
        }
    }

}


