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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
	
	
	/** The default claim buffer size */
	public static final int DEFAULT_CLAIM_BUFFER_SIZE = 1024;
	/** The default claim timeout */
	public static final long DEFAULT_CLAIM_TIMEOUT = -1;
	/** The default claim timeout unit */
	public static final TimeUnit DEFAULT_CLAIM_TIMEOUT_UNIT = TimeUnit.MILLISECONDS;
	/** The default event processor wait strategy */
	public static final WaitStrategy DEFAULT_WAIT_STRATEGY = new SleepingWaitStrategy();
	
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
     * Creates a new DisruptorEndpoint
     */
    public DisruptorEndpoint() {
    	setSynchronous(false); 
    }

    /**
     * Creates a new DisruptorEndpoint
     * @param uri The endpoint URI
     * @param component The parent component
     */
    public DisruptorEndpoint(String uri, DisruptorComponent component) {
        super(uri, component);
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
	 * Returns the configured ring buffer
	 * @return the ringBuffer
	 */
	public RingBuffer<ExchangeValueEvent> getRingBuffer() {
		return ringBuffer;
	}

	/**
	 * Sets 
	 * @param ringBuffer the ringBuffer to set
	 */
	public void setRingBuffer(RingBuffer<ExchangeValueEvent> ringBuffer) {
		this.ringBuffer = ringBuffer;
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
    
    
}
