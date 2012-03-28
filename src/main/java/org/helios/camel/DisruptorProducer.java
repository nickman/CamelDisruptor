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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangeTimedOutException;
import org.apache.camel.WaitForTaskToComplete;
import org.apache.camel.impl.DefaultAsyncProducer;
import org.apache.camel.support.SynchronizationAdapter;
import org.apache.camel.util.ExchangeHelper;
import org.helios.camel.event.ExchangeValueEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;

/**
 * <p>Title: DisruptorProducer</p>
 * <p>Description: The Disruptor producer</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.camel.DisruptorProducer</code></p>
 */
public class DisruptorProducer extends DefaultAsyncProducer {
    /** The endpoint that created this producer */
    protected final DisruptorEndpoint endpoint;
    /** The disruptor's ring buffer */
    protected final RingBuffer<ExchangeValueEvent> ringBuffer;    
	/** The endpoint's disruptor */
	protected final CamelDisruptor disruptor;
	/** The claim timeout when calling {@link RingBuffer#next(long, java.util.concurrent.TimeUnit)}. Defaults to {@link DisruptorEndpoint#DEFAULT_CLAIM_TIMEOUT}. Values of <code>< 1</code> will not have a timeout.*/
	protected final long claimTimeout;
	/** The claim timeout unit when calling {@link RingBuffer#next(long, java.util.concurrent.TimeUnit)}. Defaults to {@link DisruptorEndpoint#DEFAULT_CLAIM_TIMEOUT_UNIT}*/
	protected final TimeUnit claimTimeoutUnit;
	/** If true, a copy of the exchange will be published to the ring buffer, otherwise, the same instance will be published */
	protected final boolean copyExchange;
	/** Indicates if a claim timeout is active */
	protected final boolean timeoutActive;
	/** Indicates if the disruptor is started */
	protected boolean disruptorStarted = false;


    /**
     * Creates a new DisruptorProducer
     * @param endpoint The endpoint that created this producer
     * @param disruptor The endpoint's disruptor
     * @param copyExchange If true, a copy of the exchange will be published to the ring buffer, otherwise, the same instance will be published
     * @param claimTimeout The ringbuffer sequence claim timeout
     * @param claimTimeoutUnit The ringbuffer sequence claim timeout unit
     */
    public DisruptorProducer(DisruptorEndpoint endpoint, CamelDisruptor disruptor, boolean copyExchange, long claimTimeout, TimeUnit claimTimeoutUnit) {    	
        super(endpoint);    
        this.endpoint = endpoint;
        this.disruptor = disruptor;
        ringBuffer = this.disruptor.getRingBuffer();
        this.copyExchange = copyExchange;
        this.claimTimeout = claimTimeout;
        this.claimTimeoutUnit = claimTimeoutUnit;
        timeoutActive = claimTimeout > 0;        
        log.info("Created DisruptorProducer for endpoint [{}]", endpoint.getEndpointUri());
    }

    /**
     * Processes an exchange asynchronously by publishing it to the RingBuffer.
     * @param exchange The exchange to process
     * @param callback the {@link AsyncCallback} will be invoked when the processing of the exchange is completed. If the exchange is completed synchronously, then the callback is also invoked synchronously. The callback should therefore be careful of starting recursive loop. 
     * @return (doneSync) true to continue execute synchronously, false to continue being executed asynchronously
     */
    public boolean process(final Exchange exchange, final AsyncCallback callback) {
    	log.trace("Processing Exchange [{}]", exchange.getExchangeId());
    	checkDisruptorStarted();
        //return false;
        WaitForTaskToComplete wait = null;
        if (exchange.getProperty(Exchange.ASYNC_WAIT) != null) {
            wait = exchange.getProperty(Exchange.ASYNC_WAIT, WaitForTaskToComplete.class);
        }

        if (wait!=null && (wait == WaitForTaskToComplete.Always
            || (wait == WaitForTaskToComplete.IfReplyExpected && ExchangeHelper.isOutCapable(exchange)))) {

            // do not handover the completion as we wait for the copy to complete, and copy its result back when it done
            Exchange copy = prepareCopy(exchange, false);

            // latch that waits until we are complete
            final CountDownLatch latch = new CountDownLatch(1);

            // we should wait for the reply so install a on completion so we know when its complete
            copy.addOnCompletion(new SynchronizationAdapter() {
                @Override
                public void onDone(Exchange response) {
                    // check for timeout, which then already would have invoked the latch
                    if (latch.getCount() == 0) {
                        if (log.isTraceEnabled()) {
                            log.trace("{}. Timeout occurred so response will be ignored: {}", this, response.hasOut() ? response.getOut() : response.getIn());
                        }
                        return;
                    } else {
                        if (log.isTraceEnabled()) {
                            log.trace("{} with response: {}", this, response.hasOut() ? response.getOut() : response.getIn());
                        }
                        try {
                            ExchangeHelper.copyResults(exchange, response);
                        } finally {
                            // always ensure latch is triggered
                            latch.countDown();
                        }
                    }
                }

                @Override
                public boolean allowHandover() {
                    // do not allow handover as we want to seda producer to have its completion triggered
                    // at this point in the routing (at this leg), instead of at the very last (this ensure timeout is honored)
                    return false;
                }

                @Override
                public String toString() {
                    return "onDone at [" + endpoint.getEndpointUri() + "]";
                }
            });

            log.trace("Adding Exchange to queue: {}", copy);
            publishEvent(copy, callback);

            if (claimTimeout > 0) {
                if (log.isTraceEnabled()) {
                    log.trace("Waiting for task to complete using timeout (ms): {} at [{}]", claimTimeout, endpoint.getEndpointUri());
                }
                // lets see if we can get the task done before the timeout
                boolean done = false;
                try {
                    done = latch.await(claimTimeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    // ignore
                }
                if (!done) {
                    exchange.setException(new ExchangeTimedOutException(exchange, claimTimeout));
                    // count down to indicate timeout
                    latch.countDown();
                    // remove   timed out Exchange from queue
                    // =====================================================
                    // What is the equivalent of queue.remove(copy) ?
                    // =====================================================
                    //queue.remove(copy);
                }
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("Waiting for task to complete (blocking) at [{}]", endpoint.getEndpointUri());
                }
                // no timeout then wait until its done
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        } else {
            // no wait, eg its a InOnly then just add to queue and return
            // handover the completion so its the copy which performs that, as we do not wait
            Exchange copy = prepareCopy(exchange, true);
            log.trace("Adding Exchange to queue: {}", copy);
            publishEvent(copy, callback);
        }

        // we use OnCompletion on the Exchange to callback and wait for the Exchange to be done
        // so we should just signal the callback we are done synchronously
        callback.done(true);
        return true;
    	
    }
    
    /**
     * Starts the disruptor if not already started
     */
    protected void checkDisruptorStarted() {
    	if(!disruptorStarted) {
    		if(!disruptor.isStarted()) {
    			disruptor.start();
    		}
    	}
    	disruptorStarted = true;    	
    }
    
    /**
     * Publishes the exchange into the ring buffer
     * @param exchange the exchange to publish
     * @param callback the async callback
     */
    protected void publishEvent(final Exchange exchange, final AsyncCallback callback) {
    	disruptor.publishEvent(new EventTranslator<ExchangeValueEvent>(){
    		public ExchangeValueEvent translateTo(ExchangeValueEvent event, long sequence) {
    			event.setExchange(exchange);
    			event.setAsyncCallback(callback);
    			return event;
    		}
    	});
//    	long sequence = -1;
//    	try {
//    		sequence = timeoutActive ?  ringBuffer.next(claimTimeout, claimTimeoutUnit) : ringBuffer.next();
//    	} catch (TimeoutException te) {
//    		incrementClaimTimeouts();
//    		throw new IllegalStateException("The ring buffer in endpoint [" + endpoint.getEndpointUri() + "] timed out attempting to publish exchange [" + exchange.getExchangeId() + "]", te);
//    	}
//    	ExchangeValueEvent event = ringBuffer.get(sequence);
//    	event.setExchange(copyExchange ? prepareCopy(exchange, true) : exchange); 
//    	event.setAsyncCallback(callback);
//    	ringBuffer.publish(sequence);
    	
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

	/**
	 * {@inheritDoc}
	 * @see org.apache.camel.Processor#process(org.apache.camel.Exchange)
	 */
	@Override
	public void process(Exchange exchange) throws Exception {
    	if(!disruptorStarted) {
    		if(!disruptor.isStarted()) {
    			disruptor.start();
    		}
    	}
    	disruptorStarted = true;
    	long sequence = -1;
    	try {
    		sequence = timeoutActive ?  ringBuffer.next(claimTimeout, claimTimeoutUnit) : ringBuffer.next();
    	} catch (TimeoutException te) {
    		incrementClaimTimeouts();
    		throw new IllegalStateException("The ring buffer in endpoint [" + endpoint.getEndpointUri() + "] timed out attempting to publish exchange [" + exchange.getExchangeId() + "]", te);
    	}
    	ExchangeValueEvent event = ringBuffer.get(sequence);
    	event.setExchange(copyExchange ? prepareCopy(exchange, true) : exchange); 
    	ringBuffer.publish(sequence);
	}




}
