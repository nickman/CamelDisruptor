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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

/**
 * <p>Title: ConsumerWaitStrategy</p>
 * <p>Description: Enumerates the disruptor {@link com.lmax.disruptor.EventProcessor} wait strategies.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.camel.ConsumerWaitStrategy</code></p>
 */

public enum ConsumerWaitStrategy {
    /** Blocking strategy that uses a lock and condition variable for {@link com.lmax.disruptor.EventProcessor}s waiting on a barrier. This strategy can be used when throughput and low-latency are not as important as CPU resource. */
    BLOCK(new WaitStrategyFactory(){public WaitStrategy newInstance(){return new BlockingWaitStrategy();}}),
    /** Busy Spin strategy that uses a busy spin loop for {@link com.lmax.disruptor.EventProcessor}s waiting on a barrier. This strategy will use CPU resource to avoid syscalls which can introduce latency jitter. It is best used when threads can be bound to specific CPU cores. */
    SPIN(new WaitStrategyFactory(){public WaitStrategy newInstance(){return new BusySpinWaitStrategy();}}),
    /** Sleeping strategy that initially spins, then uses a Thread.yield(), and eventually for the minimum number of nanos the OS and JVM will allow while the {@link com.lmax.disruptor.EventProcessor}s are waiting on a barrier. This strategy is a good compromise between performance and CPU resource. Latency spikes can occur after quiet periods. */
    SLEEP(new WaitStrategyFactory(){public WaitStrategy newInstance(){return new SleepingWaitStrategy();}}),
    /** Yielding strategy that uses a Thread.yield() for {@link com.lmax.disruptor.EventProcessor}s waiting on a barrier after an initially spinning. This strategy is a good compromise between performance and CPU resource without incurring significant latency spikes.  */
    YIELD(new WaitStrategyFactory(){public WaitStrategy newInstance(){return new YieldingWaitStrategy();}});	
    
    
    /**
     * Decodes the passed name to the corresponding enum
     * @param name the name which is trimmed and uppercased
     * @return the corresponding enum
     */
    public static ConsumerWaitStrategy forName(CharSequence name) {
    	if(name==null) throw new IllegalArgumentException("The passed name was null", new Throwable());
    	String cwsName = name.toString().toUpperCase().trim();
    	try {
    		return ConsumerWaitStrategy.valueOf(cwsName);
    	} catch (Exception e) {
    		throw new IllegalArgumentException("The value [" + name + "] is not a valid ConsumerWaitStrategy name", new Throwable());
    	}
    }
    
    /**
     * Creates a new ConsumerWaitStrategy
     * @param factory a factory that creates the wait strategy for this enum option
     */
    private ConsumerWaitStrategy(WaitStrategyFactory factory) {
    	this.factory = factory;    	
    }
    
    /**
     * Returns a new WaitStrategy for this enum option
     * @return a WaitStrategy
     */
    public WaitStrategy newInstance() {
    	return factory.newInstance();
    }
    
    /** The factory for this wait strategy */
    private final WaitStrategyFactory factory;
    
    /**
     * <p>Title: WaitStrategyFactory</p>
     * <p>Description: Defines a factory for a {@link com.lmax.disruptor.WaitStrategy}</p> 
     * <p>Company: Helios Development Group LLC</p>
     * @author Whitehead (nwhitehead AT heliosdev DOT org)
     * <p><code>org.helios.camel.ConsumerWaitStrategy.WaitStrategyFactory</code></p>
     */
    private static interface WaitStrategyFactory {
    	/**
    	 * Creates a new {@link com.lmax.disruptor.WaitStrategy}
    	 * @return a {@link com.lmax.disruptor.WaitStrategy}
    	 */
    	public WaitStrategy newInstance();
    }
}
