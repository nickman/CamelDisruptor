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
package org.helios.camel.event;

import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;

import com.lmax.disruptor.EventFactory;

/**
 * <p>Title: ExchangeValueEvent</p>
 * <p>Description: The disruptor event and event factory class which wraps Camel {@link org.apache.camel.Exchange}s.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.camel.event.ExchangeValueEvent</code></p>
 */

public class ExchangeValueEvent {
	/** The event exchange */
	private Exchange exchange;
	/** The async callback indicating the exchange processing is complete */
	private AsyncCallback callback;

	/**
	 * Returns the exchange
	 * @return the exchange
	 */
	public Exchange getExchange() {
		return exchange;
	}

	/**
	 * Sets the exchange
	 * @param exchange the exchange to set
	 */
	public void setExchange(Exchange exchange) {
		this.exchange = exchange;
	}
	
	/** The disruptor event factory for ExchangeValueEvents. */
	public final static EventFactory<ExchangeValueEvent> EVENT_FACTORY = new EventFactory<ExchangeValueEvent>() {
        public ExchangeValueEvent newInstance() {
            return new ExchangeValueEvent();
        }
	};

	/**
	 * Returns the {@link AsyncCallback}
	 * @return the callback
	 */
	public AsyncCallback getAsyncCallback() {
		return callback;
	}

	/**
	 * Sets the {@link AsyncCallback}
	 * @param callback the callback to set
	 */
	public void setAsyncCallback(AsyncCallback callback) {
		this.callback = callback;
	}
	
}
