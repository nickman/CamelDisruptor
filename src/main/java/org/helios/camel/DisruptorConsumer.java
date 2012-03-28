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

import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.Processor;
import org.apache.camel.util.AsyncProcessorConverterHelper;
import org.apache.log4j.Logger;
import org.helios.camel.event.ExchangeValueEvent;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * The HelloWorld consumer.
 */
/**
 * <p>Title: DisruptorConsumer</p>
 * <p>Description: The Disruptor consumer</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.camel.DisruptorConsumer</code></p>
 */
public class DisruptorConsumer implements Consumer, EventTranslator<ExchangeValueEvent>, EventHandler<ExchangeValueEvent> {
    protected final DisruptorEndpoint endpoint;
    protected final Processor processor;
    protected final Disruptor<ExchangeValueEvent> disruptor;
    protected final Logger log;
    
    public DisruptorConsumer(DisruptorEndpoint endpoint, Processor processor) {
        this.endpoint = endpoint;
        log = Logger.getLogger(getClass().getName() + "." + endpoint.getId());
        this.processor = AsyncProcessorConverterHelper.convert(processor);
        disruptor = this.endpoint.getDisruptor();
        disruptor.handleEventsWith(this).then(this);
    	
    }

	/**
	 * {@inheritDoc}
	 * @see org.apache.camel.Service#start()
	 */
	@Override
	public void start() throws Exception {
		
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.camel.Service#stop()
	 */
	@Override
	public void stop() throws Exception {
		
	}

	/**
	 * {@inheritDoc}
	 * @see com.lmax.disruptor.EventHandler#onEvent(java.lang.Object, long, boolean)
	 */
	@Override
	public void onEvent(ExchangeValueEvent event, long sequence, boolean endOfBatch) throws Exception {
		log.info("Handling Exchange [eob:" + endOfBatch + "] Sequence:" + sequence);
		if(endOfBatch) {
			event.getAsyncCallback().done(false);
		} else {
			this.processor.process(event.getExchange());
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.lmax.disruptor.EventTranslator#translateTo(java.lang.Object, long)
	 */
	@Override
	public ExchangeValueEvent translateTo(ExchangeValueEvent event, long sequence) {
		return event;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.camel.Consumer#getEndpoint()
	 */
	@Override
	public Endpoint getEndpoint() {
		return endpoint;
	}

}
