package com.enonic.elasticsearch.discovery;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

public class SettingsProvider
{

    public Settings createNodeSettings()
        throws Exception
    {
        final ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();

        settings.putProperties( "node.", getProperties() );

        return settings.build();
    }


    private Properties getProperties()
        throws Exception
    {
        Properties prop = new Properties();

        try
        {
            InputStream in = getClass().getResourceAsStream( "node.properties" );
            prop.load( in );
            in.close();
        }
        catch ( IOException e )
        {
            System.out.println( "Not able to load settings" );
        }

        return prop;

    }

}
