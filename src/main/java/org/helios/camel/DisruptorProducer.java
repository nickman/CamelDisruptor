/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.helios.camel;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: DisruptorProducer</p>
 * <p>Description: The Disruptor producer</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.camel.DisruptorProducer</code></p>
 */
public class DisruptorProducer extends DefaultProducer {
    /** Static class logger */
    private static final transient Logger LOG = LoggerFactory.getLogger(DisruptorProducer.class);
    /** The disruptor endpoint messages are being routed to */
    private DisruptorEndpoint endpoint;

    public DisruptorProducer(DisruptorEndpoint endpoint) {    	
        super(endpoint);        
        this.endpoint = endpoint;
        if(LOG.isDebugEnabled()) LOG.info("Created DisruptorProducer for endpoint [" + this.endpoint.getEndpointUri() + "]");
    }

    public void process(Exchange exchange) throws Exception {
        System.out.println(exchange.getIn().getBody());    
    }

}
