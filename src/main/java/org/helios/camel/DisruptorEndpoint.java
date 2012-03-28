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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.MultipleConsumersSupport;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.api.management.ManagedAttribute;
import org.apache.camel.api.management.ManagedOperation;
import org.apache.camel.api.management.ManagedResource;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.log4j.Logger;

import com.lmax.disruptor.RingBuffer;

/**
 * <p>Title: DisruptorEndpoint</p>
 * <p>Description: Represents a Disruptor endpoint.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.camel.DisruptorEndpoint</code></p>
 */
@ManagedResource(description = "Managed DisruptorEndpoint")
public class DisruptorEndpoint extends DefaultEndpoint implements MultipleConsumersSupport {
	/** The endpoint's disruptor */
	protected CamelDisruptor disruptor;
	/** The claim timeout when calling {@link RingBuffer#next(long, java.util.concurrent.TimeUnit)}. Defaults to {@link DisruptorEndpoint#DEFAULT_CLAIM_TIMEOUT}. Values of <code>< 1</code> will not have a timeout.*/
	protected long claimTimeout = -1;
	/** The claim timeout unit when calling {@link RingBuffer#next(long, java.util.concurrent.TimeUnit)}. Defaults to {@link DisruptorEndpoint#DEFAULT_CLAIM_TIMEOUT_UNIT}*/
	protected TimeUnit claimTimeoutUnit = null;
	/** If true, a copy of the exchange will be published to the ring buffer, otherwise, the same instance will be published */
	protected boolean copyExchange;	
	/** The executor thread pool built to execute event processors */
	protected Executor threadPool = null;
	/** A set of the current disruptor producers */
    private final Set<DisruptorProducer> producers = new CopyOnWriteArraySet<DisruptorProducer>();
    /** A set of the current disruptor consumers */
    private final Set<DisruptorConsumer> consumers = new CopyOnWriteArraySet<DisruptorConsumer>();	
	/** Claim timeout counter */
	protected final AtomicLong claimTimeouts = new AtomicLong(0L);
	/** Instance logger */
	protected final Logger log;
	
    /**
     * Creates a new DisruptorEndpoint
     * @param uri The endpoint URI
     * @param component The parent component
     * @param copyExchange If true, a copy of the exchange will be published to the ring buffer, otherwise, the same instance will be published
     * @param timeout The publisher timeout 
     * @param unit The publisher timeout unit
     */
    public DisruptorEndpoint(String uri, Component component, boolean copyExchange, long timeout, TimeUnit unit) {
        super(uri, component);
        log = Logger.getLogger(getClass().getName() + "-" + this.getEndpointKey());
        log.info("Creating Disruptor Endpoint [" + uri + "]");
    	this.copyExchange = copyExchange;
    	this.claimTimeout = timeout;
    	this.claimTimeoutUnit = unit;
        setSynchronous(false);     	 
    }


    /**
     * {@inheritDoc}
     * @see org.apache.camel.Endpoint#createProducer()
     */
    public Producer createProducer() throws Exception {
    	log.info("Creating Disruptor Procuer");
    	DisruptorProducer producer = new DisruptorProducer(this, disruptor, copyExchange, claimTimeout, claimTimeoutUnit);
    	producers.add(producer);
    	return producer;
    }

    /**
     * {@inheritDoc}
     * @see org.apache.camel.Endpoint#createConsumer(org.apache.camel.Processor)
     */
    public Consumer createConsumer(Processor processor) throws Exception {
    	log.info("Creating Disruptor Consumer for [" + processor + "]");
    	DisruptorConsumer consumer = new DisruptorConsumer(this, processor);
    	consumers.add(consumer);
    	return consumer;
    }

    /**
     * {@inheritDoc}
     * @see org.apache.camel.IsSingleton#isSingleton()
     */
    public boolean isSingleton() {
        return true;
    }
    
    /**
     * The capacity of the ring buffer to hold entries.
     * @return the size of the RingBuffer.
     */
    public int getBufferSize() {
        return disruptor.getRingBuffer().getBufferSize();
    }    
    
    /**
     * {@inheritDoc}
     * @see org.apache.camel.impl.DefaultEndpoint#configureProperties(java.util.Map)
     */
    @Override
    public void configureProperties(Map<String, Object> options) {
    	disruptor = (CamelDisruptor)options.get("disruptor");
    	threadPool = disruptor.getExecutor();
    }    
    
    
    /**
     * {@inheritDoc}
     * @see org.apache.camel.impl.DefaultEndpoint#doStart()
     */
    protected void doStart() {
    	
    	//disruptor.start();
    }
    
    /**
     * {@inheritDoc}
     * @see org.apache.camel.impl.DefaultEndpoint#doStop()
     */
    protected void doStop() {
    	log.info("Stopping Disruptor");
    	disruptor.shutdown();
    }
    
    
    /**
     * @param producer
     */
    void onStarted(DisruptorProducer producer) {
        producers.add(producer);
    }

    /**
     * @param producer
     */
    void onStopped(DisruptorProducer producer) {
        producers.remove(producer);
    }
    
    
    /**
     * @param consumer
     * @throws Exception
     */
    void onStarted(DisruptorConsumer consumer) throws Exception {
        consumers.add(consumer);
//        if (isMultipleConsumers()) {
//            updateMulticastProcessor();
//        }
    }

    /**
     * @param consumer
     * @throws Exception
     */
    void onStopped(DisruptorConsumer consumer) throws Exception {
        consumers.remove(consumer);
//        if (isMultipleConsumers()) {
//            updateMulticastProcessor();
//        }
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
	 * Returns the event processor thread pool
	 * @return the event processor thread pool
	 */
	public Executor getThreadPool() {
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
     * Increments the timeout count for claim failures
     */
    void incrementClaimTimeouts() {
    	claimTimeouts.incrementAndGet();
    }
	
    /**
     * Returns the number of claim timeouts since the last reset
     * @return the number of claim timeouts since the last reset
     */
    @ManagedAttribute(description="The number of claim timeouts since the last reset")
    public long getClaimTimeouts() {
    	return claimTimeouts.get();
    }
    
    /**
     * Resets the endpoint metrics
     */
    @ManagedOperation(description="Resets the endpoint metrics")
    public void resetMetrics() {
    	claimTimeouts.set(0L);
    }

	/**
	 * Constructs a <code>String</code> with all attributes in <code>name:value</code> format.
	 * @return a <code>String</code> representation of this object.
	 */
	public String toString() {
	    final String TAB = "\n\t";
	    StringBuilder retValue = new StringBuilder();    
	    retValue.append("DisruptorEndpoint [")
		    .append(TAB).append("disruptor:").append(this.disruptor)
		    .append(TAB).append("claimTimeout:").append(this.claimTimeout)
		    .append(TAB).append("claimTimeoutUnit:").append(this.claimTimeoutUnit)
		    .append(TAB).append("threadPool:").append(this.threadPool)
	    	.append("\n]");    
	    return retValue.toString();
	}


	/**
	 * {@inheritDoc}
	 * @see org.apache.camel.MultipleConsumersSupport#isMultipleConsumersSupported()
	 */
	@Override
	public boolean isMultipleConsumersSupported() {
		return true;
	}


	/**
	 * Returns 
	 * @return the disruptor
	 */
	public CamelDisruptor getDisruptor() {
		return disruptor;
	}


	/**
	 * Sets 
	 * @param disruptor the disruptor to set
	 */
	public void setDisruptor(CamelDisruptor disruptor) {
		this.disruptor = disruptor;
	}
    
    
}
