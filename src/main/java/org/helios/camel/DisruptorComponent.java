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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.helios.camel.event.ExchangeValueEvent;

import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * <p>Title: DisruptorComponent</p>
 * <p>Description: Represents the component that manages {@link DisruptorEndpoint}.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.camel.DisruptorComponent</code></p>
 */
public class DisruptorComponent extends DefaultComponent {

	/** The default claim buffer size */
	public static final int DEFAULT_CLAIM_BUFFER_SIZE = 1024;
	/** The default claim timeout */
	public static final long DEFAULT_CLAIM_TIMEOUT = -1;
	/** The default claim timeout unit */
	public static final TimeUnit DEFAULT_CLAIMTIMEOUTUNIT = TimeUnit.MILLISECONDS;
	/** The default event processor wait strategy */
	public static final ConsumerWaitStrategy DEFAULT_WAIT_STRATEGY = ConsumerWaitStrategy.SLEEP;
	/** The default exchange copy */
	public static final boolean DEFAULT_COPYEXCHANGE = true;
	
	
    /** URI option name for Claim Buffer Size */
    public static final String OPTION_CLAIMBUFFERSIZE = "size";
    /** URI option name for Claim Timeout */
    public static final String OPTION_CLAIMTIMEOUT = "timeout";
    /** URI option name for Claim Timeout Unit*/
    public static final String OPTION_CLAIMTIMEOUTUNIT = "unit";
    /** URI option name for Force single threaded prodcuer */
    public static final String OPTION_SINGLEPUB = "singlepub";
    /** URI option name for event processor wait strategy */
    public static final String OPTION_WAITSTRAT = "waitstrat";
	/** URI option name for  exchange copy */
	public static final String OPTION_COPYEXCHANGE = "copy";
	
	
	/** A map of created Disruptor resident and referenced in disruptor endpoints */
	private final Map<String, Disruptor<ExchangeValueEvent>> disruptors = new HashMap<String, Disruptor<ExchangeValueEvent>>();
	
	/**
	 * Creates a new DisruptorComponent
	 */
	public DisruptorComponent() {
		super();
	}

	/**
	 * Creates a new DisruptorComponent
	 * @param context The CamelContext
	 */
	public DisruptorComponent(CamelContext context) {
	        super(context);
	}
	
    public CamelContext getCamelContext() {
        return super.getCamelContext();
    }

    public void setCamelContext(CamelContext context) {
        super.setCamelContext(context);
    }	
    /**
     * Creates or retrieves the disruptor identified and confirable by the passed parameters
     * @param uri The component URI
     * @param parameters The parsed URI parameters
     * @return the created or cached disruptor
     */
    public synchronized Disruptor<ExchangeValueEvent> createDisruptor(String uri, final Map<String, Object> parameters, DisruptorEndpoint de ) {
        String key = getDisruptorKey(uri);
        if (disruptors.containsKey(key)) {
            return disruptors.get(key);
        }
        // create ring buffer
        Disruptor<ExchangeValueEvent> disruptor;
        int cbs = getAndRemoveParameter(parameters, OPTION_CLAIMBUFFERSIZE, Integer.class, DEFAULT_CLAIM_BUFFER_SIZE);
    	boolean forceSinglePub = getAndRemoveParameter(parameters, OPTION_SINGLEPUB, Boolean.class, false);
    	ConsumerWaitStrategy waitStrat = ConsumerWaitStrategy.valueOf(getAndRemoveParameter(parameters, OPTION_WAITSTRAT, String.class, DEFAULT_WAIT_STRATEGY.name()).toUpperCase().trim());
    	disruptor = new CamelDisruptor(
    			ExchangeValueEvent.EVENT_FACTORY,
    			de,
    			this.getCamelContext(),    			    		
    			forceSinglePub ? new SingleThreadedClaimStrategy(cbs) : new MultiThreadedClaimStrategy(cbs),
    		    waitStrat.newInstance()
    	);
    	parameters.put("disruptor", disruptor);
    	disruptors.put(key, disruptor);
        return disruptor;
    }
    
    /**
     * {@inheritDoc}
     * @see org.apache.camel.impl.DefaultComponent#createEndpoint(java.lang.String, java.lang.String, java.util.Map)
     */
    @Override
    protected Endpoint createEndpoint(String uri, String remaining, final Map<String, Object> parameters) throws Exception {
    	long timeout = getAndRemoveParameter(parameters, OPTION_CLAIMTIMEOUT, Long.class, DEFAULT_CLAIM_TIMEOUT);
    	TimeUnit unit = TimeUnit.valueOf(getAndRemoveParameter(parameters, OPTION_CLAIMTIMEOUTUNIT, String.class, DEFAULT_CLAIMTIMEOUTUNIT.name()).toUpperCase().trim());
    	boolean copyExchange = getAndRemoveParameter(parameters, OPTION_COPYEXCHANGE, boolean.class, DEFAULT_COPYEXCHANGE);
    	DisruptorEndpoint de = new DisruptorEndpoint(uri, this, copyExchange, timeout, unit);
    	createDisruptor(uri, parameters, de);
    	de.configureProperties(parameters);
        return de;
    }
    
    
    
	
    /**
     * Extracts the unique key of the Disruptor to created
     * @param uri The component URI
     * @return the ring buffer key
     */
    protected String getDisruptorKey(String uri) {
        if (uri.contains("?")) {
            // strip parameters
            uri = uri.substring(0, uri.indexOf('?'));
        }
        return uri;
    }    
    

}
