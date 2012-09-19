package com.enonic.elasticsearch.discovery;

/**
 * Hello world!
 */
public class Main
{
    public static void main( String[] args )
        throws Exception
    {
        startInstance();
    }

    private static void startInstance()
        throws Exception
    {
        ElasticsearchInstance instance = new ElasticsearchInstance();
        instance.start();
        Poller poller = new Poller( instance );
        poller.poll( 0 );
        instance.stop();
    }



}
