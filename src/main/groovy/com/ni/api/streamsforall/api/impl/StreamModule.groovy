package com.ni.api.streamsforall.api.impl

import com.google.inject.AbstractModule
import com.google.inject.Scopes

public class StreamModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(StreamService.class).in(Scopes.SINGLETON)
    }

}
