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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.helios.camel.event.ExchangeValueEvent;

import com.lmax.disruptor.ClaimStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;

/**
 * <p>Title: DisruptorEndpoint</p>
 * <p>Description: Represents a Disruptor endpoint.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.camel.DisruptorEndpoint</code></p>
 */
public class DisruptorEndpoint extends DefaultEndpoint {
	/** The endpoint's ringbuffer */
	protected RingBuffer<ExchangeValueEvent> ringBuffer;
	/** Indicates if the endpoint should force a single publisher model and synchronize incoming events. Defaults to false */
	protected boolean forceSinglePub = false;
	/** The sequence claim strategy */
	protected ClaimStrategy claimStrategy = null;
	/** The sequence claim buffer size. Defaults to {@link DisruptorEndpoint#DEFAULT_CLAIM_BUFFER_SIZE} */
	protected int claimBufferSize = DEFAULT_CLAIM_BUFFER_SIZE;
	/** The claim timeout when calling {@link RingBuffer#next(long, java.util.concurrent.TimeUnit)}. Defaults to {@link DisruptorEndpoint#DEFAULT_CLAIM_TIMEOUT}. Values of <code>< 1</code> will not have a timeout.*/
	protected long claimTimeout = -1;
	/** The claim timeout unit when calling {@link RingBuffer#next(long, java.util.concurrent.TimeUnit)}. Defaults to {@link DisruptorEndpoint#DEFAULT_CLAIM_TIMEOUT_UNIT}*/
	protected TimeUnit claimTimeoutUnit = DEFAULT_CLAIM_TIMEOUT_UNIT;
	/** The configured event processor event wait strategy instance. */
	protected WaitStrategy waitStrategy = null;
	/** The event processor wait strategy. Defaults to {@link ConsumerWaitStrategy#SLEEP} */
	protected ConsumerWaitStrategy consumerWaitStrategy = ConsumerWaitStrategy.SLEEP;
	/** The executor thread pool built to execute event processors */
	protected ThreadPoolExecutor threadPool = null;
	/** The logical name of this endpoint */
	protected String endpointName = null;
	/** The endpoint serial number*/
	protected final long endpointSerial = serial.incrementAndGet();
	
	
	/** A serial number generator for assigning to endpoints */
	protected static final AtomicLong serial = new AtomicLong(0L);
	
	/** The default claim buffer size */
	public static final int DEFAULT_CLAIM_BUFFER_SIZE = 1024;
	/** The default claim timeout */
	public static final long DEFAULT_CLAIM_TIMEOUT = -1;
	/** The default claim timeout unit */
	public static final TimeUnit DEFAULT_CLAIM_TIMEOUT_UNIT = TimeUnit.MILLISECONDS;
	/** The default event processor wait strategy */
	public static final WaitStrategy DEFAULT_WAIT_STRATEGY = new SleepingWaitStrategy();
	
    /** URI option name for Claim Buffer Size */
    public static final String OPTION_CLAIMBUFFERSIZE = "claimbuffsize";
    /** URI option name for Claim Timeout */
    public static final String OPTION_CLAIMTIMEOUT = "claimto";
    /** URI option name for Claim Timeout Unit*/
    public static final String OPTION_CLAIMTIMEOUTUNIT = "claimtounit";
    /** URI option name for Force single threaded prodcuer */
    public static final String OPTION_SINGLEPUB = "singlepub";
    /** URI option name for event processor wait strategy */
    public static final String OPTION_WAITSTRAT = "waitstrat";
	
	
	/*
	 * Configuration:
	 * ==============
	 * Ring Configuration
	 * 		Forced singleton (single producer)
	 * 		Claim Strategy and Buffer Size
	 * 		Claim Timeout
	 * 		
	 * Executor Pool
	 * Wait Strategy
	 */
	
    /**
     * Creates a new DisruptorEndpoint with a default name
     */
    public DisruptorEndpoint() {
    	setSynchronous(false); 
    	endpointName = getClass().getSimpleName() + "#" + endpointSerial;
    }

    /**
     * Creates a new DisruptorEndpoint
     * @param uri The endpoint URI
     * @param component The parent component
     */
    public DisruptorEndpoint(String uri, DisruptorComponent component) {
        super(uri, component);
    	setSynchronous(false); 
    	endpointName = getClass().getSimpleName() + "#" + endpointSerial;        
    }


    /**
     * {@inheritDoc}
     * @see org.apache.camel.Endpoint#createProducer()
     */
    public Producer createProducer() throws Exception {
        return new DisruptorProducer(this);
    }

    /**
     * {@inheritDoc}
     * @see org.apache.camel.Endpoint#createConsumer(org.apache.camel.Processor)
     */
    public Consumer createConsumer(Processor processor) throws Exception {
        return new DisruptorConsumer(this, processor);
    }

    /**
     * {@inheritDoc}
     * @see org.apache.camel.IsSingleton#isSingleton()
     */
    public boolean isSingleton() {
        return true;
    }
    
    /**
     * {@inheritDoc}
     * @see org.apache.camel.impl.DefaultEndpoint#configureProperties(java.util.Map)
     */
    @Override
    public void configureProperties(Map<String, Object> options) {
    	Map<String, Object> caseInsOptions = new HashMap<String, Object>();
    	for(Map.Entry<String, Object> entry: options.entrySet()) {
    		caseInsOptions.put(entry.getKey().trim().toLowerCase(), entry.getValue());
    	}
    	Integer cbs = (Integer)caseInsOptions.get(OPTION_CLAIMBUFFERSIZE);
    	if(cbs!=null) {
    		claimBufferSize = cbs;
    	}
    	Long cto = (Long)caseInsOptions.get(OPTION_CLAIMBUFFERSIZE);
    	if(cto!=null) {
    		claimTimeout = cto;
    	}
    	
    	Object unit = caseInsOptions.get(OPTION_CLAIMTIMEOUTUNIT);
    	if(unit!=null) {
    		claimTimeoutUnit = TimeUnit.valueOf(unit.toString().trim().toUpperCase());
    	}
    	
    	Object force = caseInsOptions.get(OPTION_SINGLEPUB);
    	if(force!=null) {
    		forceSinglePub = Boolean.parseBoolean(force.toString().trim());
    	} 
    	
    	Object waitStrat = caseInsOptions.get(OPTION_WAITSTRAT);
    	if(waitStrat!=null) {
    		consumerWaitStrategy = ConsumerWaitStrategy.valueOf(waitStrat.toString().trim().toUpperCase());
    	}
    }    
    
    
    /**
     * {@inheritDoc}
     * @see org.apache.camel.impl.DefaultEndpoint#doStart()
     */
    protected void doStart() {
    	
    }
    
    protected void doStop() {
    	
    }
    
    
    /**
     * @param producer
     */
    void onStarted(DisruptorProducer producer) {
//        producers.add(producer);
    }

    /**
     * @param producer
     */
    void onStopped(DisruptorProducer producer) {
//        producers.remove(producer);
    }
    
    
    /**
     * @param consumer
     * @throws Exception
     */
    void onStarted(DisruptorConsumer consumer) throws Exception {
//        consumers.add(consumer);
//        if (isMultipleConsumers()) {
//            updateMulticastProcessor();
//        }
    }

    /**
     * @param consumer
     * @throws Exception
     */
    void onStopped(DisruptorConsumer consumer) throws Exception {
//        consumers.remove(consumer);
//        if (isMultipleConsumers()) {
//            updateMulticastProcessor();
//        }
    }

	/**
	 * Indicates if the component is forcing a single ring buffer producer
	 * @return the forceSinglePub
	 */
	public boolean isForceSinglePub() {
		return forceSinglePub;
	}

	/**
	 * Forces a single ring buffer producer and synchronizes camel producers.
	 * If this is not forced, the claim strategy will be set according to the number of configured producers when the endpoint starts. 
	 * @param forceSinglePub the forceSinglePub to set
	 */
	public void setForceSinglePub(boolean forceSinglePub) {
		this.forceSinglePub = forceSinglePub;
	}

	/**
	 * Returns an expression describing the claim strategy
	 * @return the claimStrategy
	 */
	public String getClaimStrategy() {
		return claimStrategy.getClass().getSimpleName() + "[" + claimStrategy.getBufferSize() + "]";
	}


	/**
	 * Returns the configured claim buffer size 
	 * @return the claimBufferSize
	 */
	public int getClaimBufferSize() {
		return claimBufferSize;
	}

	/**
	 * Sets the claim buffer size
	 * @param claimBufferSize the claim Buffer Size to set
	 */
	public void setClaimBufferSize(int claimBufferSize) {
		this.claimBufferSize = claimBufferSize;
	}

	/**
	 * Returns the event processor claim timeout
	 * @return the event processor claim timeout
	 */
	public long getClaimTimeout() {
		return claimTimeout;
	}

	/**
	 * Sets the event processor claim timeout
	 * @param claimTimeout the event processor claim timeout
	 */
	public void setClaimTimeout(long claimTimeout) {
		this.claimTimeout = claimTimeout;
	}

	/**
	 * Returns the event processor claim timeout unit
	 * @return the event processor claim timeout unit
	 */
	public TimeUnit getClaimTimeoutUnit() {
		return claimTimeoutUnit;
	}

	/**
	 * Sets the event processor claim timeout unit
	 * @param claimTimeoutUnit the event processor claim timeout unit
	 */
	public void setClaimTimeoutUnit(TimeUnit claimTimeoutUnit) {
		this.claimTimeoutUnit = claimTimeoutUnit;
	}

	/**
	 * Returns 
	 * @return the waitStrategy
	 */
	public WaitStrategy getWaitStrategy() {
		return waitStrategy;
	}

	/**
	 * Sets the wait strategy that will be used by event processors
	 * @param waitStrategy the wait Strategy to set
	 */
	public void setWaitStrategy(ConsumerWaitStrategy waitStrategy) {
		this.consumerWaitStrategy = waitStrategy;
	}
	
	/**
	 * Sets the wait strategy enum name that will be used by event processors
	 * @param waitStrategyName the wait Strategy enum name to set
	 */
	public void setWaitStrategy(CharSequence waitStrategyName) {
		this.consumerWaitStrategy = ConsumerWaitStrategy.forName(waitStrategyName);
	}
	

	/**
	 * Returns the event processor thread pool
	 * @return the event processor thread pool
	 */
	public ThreadPoolExecutor getThreadPool() {
		return threadPool;
	}

	/**
	 * Sets the event processor thread pool
	 * @param threadPool the event processor thread pool
	 */
	public void setThreadPool(ThreadPoolExecutor threadPool) {
		this.threadPool = threadPool;
	}

	/**
	 * Constructs a <code>String</code> with all attributes in <code>name:value</code> format.
	 * @return a <code>String</code> representation of this object.
	 */
	public String toString() {
	    final String TAB = "\n\t";
	    StringBuilder retValue = new StringBuilder();    
	    retValue.append("DisruptorEndpoint [")
		    .append(TAB).append("ringBuffer:").append(this.ringBuffer)
		    .append(TAB).append("forceSinglePub:").append(this.forceSinglePub)
		    .append(TAB).append("claimStrategy:").append(this.claimStrategy)
		    .append(TAB).append("claimBufferSize:").append(this.claimBufferSize)
		    .append(TAB).append("claimTimeout:").append(this.claimTimeout)
		    .append(TAB).append("claimTimeoutUnit:").append(this.claimTimeoutUnit)
		    .append(TAB).append("waitStrategy:").append(this.waitStrategy)
		    .append(TAB).append("consumerWaitStrategy:").append(this.consumerWaitStrategy)
		    .append(TAB).append("threadPool:").append(this.threadPool)
	    	.append("\n]");    
	    return retValue.toString();
	}
    
    
}
