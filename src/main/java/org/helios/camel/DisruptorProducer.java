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

import java.util.concurrent.TimeUnit;

import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultAsyncProducer;
import org.apache.camel.util.ExchangeHelper;
import org.helios.camel.event.ExchangeValueEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * <p>Title: DisruptorProducer</p>
 * <p>Description: The Disruptor producer</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.camel.DisruptorProducer</code></p>
 */
public class DisruptorProducer extends DefaultAsyncProducer  {
    /** Static class logger */
    private static final transient Logger LOG = LoggerFactory.getLogger(DisruptorProducer.class);
    /** The endpoint that created this producer */
    protected final DisruptorEndpoint endpoint;
    /** The disruptor's ring buffer */
    protected final RingBuffer<ExchangeValueEvent> ringBuffer;    
	/** The endpoint's disruptor */
	protected final Disruptor<ExchangeValueEvent> disruptor;
	/** The claim timeout when calling {@link RingBuffer#next(long, java.util.concurrent.TimeUnit)}. Defaults to {@link DisruptorEndpoint#DEFAULT_CLAIM_TIMEOUT}. Values of <code>< 1</code> will not have a timeout.*/
	protected final long claimTimeout;
	/** The claim timeout unit when calling {@link RingBuffer#next(long, java.util.concurrent.TimeUnit)}. Defaults to {@link DisruptorEndpoint#DEFAULT_CLAIM_TIMEOUT_UNIT}*/
	protected final TimeUnit claimTimeoutUnit;
	/** If true, a copy of the exchange will be published to the ring buffer, otherwise, the same instance will be published */
	protected final boolean copyExchange;
	/** Indicates if a claim timeout is active */
	protected final boolean timeoutActive;


    /**
     * Creates a new DisruptorProducer
     * @param endpoint The endpoint that created this producer
     * @param disruptor The endpoint's disruptor
     * @param copyExchange If true, a copy of the exchange will be published to the ring buffer, otherwise, the same instance will be published
     * @param claimTimeout The ringbuffer sequence claim timeout
     * @param claimTimeoutUnit The ringbuffer sequence claim timeout unit
     */
    public DisruptorProducer(DisruptorEndpoint endpoint, Disruptor<ExchangeValueEvent> disruptor, boolean copyExchange, long claimTimeout, TimeUnit claimTimeoutUnit) {    	
        super(endpoint);    
        this.endpoint = endpoint;
        this.disruptor = disruptor;
        ringBuffer = this.disruptor.getRingBuffer();
        this.copyExchange = copyExchange;
        this.claimTimeout = claimTimeout;
        this.claimTimeoutUnit = claimTimeoutUnit;
        timeoutActive = claimTimeout > 0;        
        if(LOG.isDebugEnabled()) LOG.info("Created DisruptorProducer for endpoint [" + getEndpoint().getEndpointUri() + "]");
    }

    /**
     * Processes an exchange asynchronously by publishing it to the RingBuffer.
     * @param exchange The exchange to process
     * @param callback the {@link AsyncCallback} will be invoked when the processing of the exchange is completed. If the exchange is completed synchronously, then the callback is also invoked synchronously. The callback should therefore be careful of starting recursive loop. 
     * @return (doneSync) true to continue execute synchronously, false to continue being executed asynchronously
     */
    public boolean process(final Exchange exchange, final AsyncCallback callback) {
    	long sequence = -1;
    	try {
    		sequence = timeoutActive ?  ringBuffer.next(claimTimeout, claimTimeoutUnit) : ringBuffer.next();
    	} catch (TimeoutException te) {
    		incrementClaimTimeouts();
    		throw new IllegalStateException("The ring buffer in endpoint [" + endpoint.getEndpointUri() + "] timed out attempting to publish exchange [" + exchange.getExchangeId() + "]", te);
    	}
    	ExchangeValueEvent event = ringBuffer.get(sequence);
    	event.setExchange(copyExchange ? prepareCopy(exchange, true) : exchange); 
    	event.setAsyncCallback(callback);
    	ringBuffer.publish(sequence);
        return false;
    }
    
    
    
    /**
     * {@inheritDoc}
     * @see org.apache.camel.impl.DefaultProducer#doStart()
     */
    @Override
    protected void doStart() throws Exception {
        super.doStart();
        endpoint.onStarted(this);
    }

    /**
     * {@inheritDoc}
     * @see org.apache.camel.impl.DefaultProducer#doStop()
     */
    @Override
    protected void doStop() throws Exception {
        endpoint.onStopped(this);
        super.doStop();
    }    
    
    /**
     * Creates a correlated copy of the exchange
     * @param exchange The exchange to copy
     * @param handover whether the on completion callbacks should be handed over to the new copy.
     * @return the exchange copy
     */
    protected Exchange prepareCopy(Exchange exchange, boolean handover) {
        Exchange copy = ExchangeHelper.createCorrelatedCopy(exchange, handover);
        copy.setFromEndpoint(endpoint);
        return copy;
    }    
    
    /**
     * Increments the timeout count for claim failures
     */
    protected void incrementClaimTimeouts() {
    	endpoint.incrementClaimTimeouts();
    }


}
