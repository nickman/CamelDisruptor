/**
 * Helios, OpenSource Monitoring
 * Brought to you by the Helios Development Group
 *
 * Copyright 2007, Helios Development Group and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org. 
 *
 */
package org.helios.camel;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.camel.CamelContext;
import org.helios.camel.event.ExchangeValueEvent;

import com.lmax.disruptor.ClaimStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * <p>Title: CamelDisruptor</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.camel.CamelDisruptor</code></p>
 */

public class CamelDisruptor extends Disruptor<ExchangeValueEvent>  {
	/** The camel context */
	protected CamelContext camelContext;
	/** The disruptor's event processor executor */
	protected Executor executor;
	/** Stupid hack to keep a ref to the created executor since it is private in the parent class */
	protected static final ThreadLocal<Executor> createdExecutor = new ThreadLocal<Executor>();
	/** Started state flag */
	protected final AtomicBoolean started = new AtomicBoolean(false);
	
	
	
	/**
	 * Creates a new CamelDisruptor
     * @param eventFactory   the factory to create events in the ring buffer.
     * @param endpoint		 the endpoint for which this disruptor is being created
     * @param camelContext	 the camel context withing which the disruptor is being managed
     * @param claimStrategy  the claim strategy to use for the ring buffer.
     * @param waitStrategy   the wait strategy to use for the ring buffer.
	 */
	public CamelDisruptor(EventFactory<ExchangeValueEvent> eventFactory, DisruptorEndpoint endpoint, CamelContext camelContext, ClaimStrategy claimStrategy, WaitStrategy waitStrategy) {
		super(eventFactory, getExecutor(camelContext, endpoint), claimStrategy, waitStrategy);
		executor = createdExecutor.get();
		createdExecutor.remove();
	}
	
	/**
	 * Creates a new CamelDisruptor
     * @param eventFactory   the factory to create events in the ring buffer.
     * @param ringBufferSize the size of the ring buffer.
     * @param endpoint		 the endpoint for which this disruptor is being created
     * @param camelContext	 the camel context withing which the disruptor is being managed
	 */
	public CamelDisruptor(EventFactory<ExchangeValueEvent> eventFactory, int ringBufferSize, DisruptorEndpoint endpoint, CamelContext camelContext) {
		super(eventFactory, ringBufferSize, getExecutor(camelContext, endpoint));	
		executor = createdExecutor.get();
		createdExecutor.remove();		
	}
	
	public RingBuffer<ExchangeValueEvent> start() {
		RingBuffer<ExchangeValueEvent> rb = super.start();
		started.set(true);
		return rb;
	}
	
	public boolean isStarted() {
		return started.get();
	}
	
	/**
	 * Creates an executor through the camel comtext's executor service manager
	 * @param camelContext The camel context providing the camel executor service manager
	 * @param endpoint The endpoint for which the executor is being created
	 * @return an executor
	 */
	protected static Executor getExecutor(CamelContext camelContext, DisruptorEndpoint endpoint ) {
		Executor executor = camelContext.getExecutorServiceManager().newCachedThreadPool(endpoint, "Disruptor");
		createdExecutor.set(executor);
		return executor;
	}

	

}
