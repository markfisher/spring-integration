/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.channel;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.integration.Message;
import org.springframework.integration.MessageHeaders;
import org.springframework.integration.util.UpperBound;

/**
 * A message channel that prioritizes messages based on a {@link Comparator}.
 * The default comparator is based upon the message header's 'priority'.
 * 
 * @author Mark Fisher
 */
public class PriorityChannel extends QueueChannel {

	private static final String SEQUENCE_HEADER_NAME = "__priorityChannelSequence__";


	private final AtomicLong counter = new AtomicLong();

	private final UpperBound upperBound;


	/**
	 * Create a channel with the specified queue capacity. If the capacity
	 * is a non-positive value, the queue will be unbounded. Message priority
	 * will be determined by the provided {@link Comparator}. If the comparator
	 * is <code>null</code>, the priority will be based upon the value of
	 * {@link MessageHeaders#getPriority()}.
	 */
	public PriorityChannel(int capacity, Comparator<Message<?>> comparator) {
		super(new PriorityBlockingQueue<Message<?>>(11, new SequenceFallbackComparator(comparator)));
		this.upperBound = new UpperBound(capacity);
	}

	/**
	 * Create a channel with the specified queue capacity. Message priority
	 * will be based upon the value of {@link MessageHeaders#getPriority()}.
	 */
	public PriorityChannel(int capacity) {
		this(capacity, null);
	}

	/**
	 * Create a channel with an unbounded queue. Message priority will be
	 * determined by the provided {@link Comparator}. If the comparator
	 * is <code>null</code>, the priority will be based upon the value of
	 * {@link MessageHeaders#getPriority()}.
	 */
	public PriorityChannel(Comparator<Message<?>> comparator) {
		this(0, comparator);
	}

	/**
	 * Create a channel with an unbounded queue. Message priority will be
	 * based on the value of {@link MessageHeaders#getPriority()}.
	 */
	public PriorityChannel() {
		this(0, null);
	}


	@Override
	protected boolean doSend(Message<?> message, long timeout) {
		if (!upperBound.tryAcquire(timeout)) {
			return false;
		}
		DirectFieldAccessor accessor = new DirectFieldAccessor(message.getHeaders());
		Map headers = (Map) accessor.getPropertyValue("headers");
		headers.put(SEQUENCE_HEADER_NAME, counter.incrementAndGet());
		return super.doSend(message, 0);
	}

	@Override
	protected Message<?> doReceive(long timeout) {
		Message<?> message = super.doReceive(timeout);
		if (message != null) {
			DirectFieldAccessor accessor = new DirectFieldAccessor(message.getHeaders());
			Map headers = (Map) accessor.getPropertyValue("headers");
			headers.remove(SEQUENCE_HEADER_NAME);
			upperBound.release();
			return message;
		}
		return null;
	}


	private static class MessagePriorityComparator implements Comparator<Message<?>> {

		public int compare(Message<?> message1, Message<?> message2) {
			Integer priority1 = message1.getHeaders().getPriority();
			Integer priority2 = message2.getHeaders().getPriority();
			priority1 = priority1 != null ? priority1 : 0;
			priority2 = priority2 != null ? priority2 : 0;
			return priority2.compareTo(priority1);
		}
	}


	private static class SequenceFallbackComparator implements Comparator<Message<?>> {

		private final Comparator<Message<?>> delegate;

		private SequenceFallbackComparator(Comparator<Message<?>> delegate) {
			if (delegate == null) {
				delegate = new MessagePriorityComparator();
			}
			this.delegate = delegate;
		}

		public int compare(Message<?> message1, Message<?> message2) {
			int result = this.delegate.compare(message1, message2);
			if (result == 0) {
				Long sequence1 = message1.getHeaders().get(SEQUENCE_HEADER_NAME, Long.class);
				Long sequence2 = message2.getHeaders().get(SEQUENCE_HEADER_NAME, Long.class);
				sequence1 = sequence1 != null ? sequence1 : 0;
				sequence2 = sequence2 != null ? sequence2 : 0;
				result = sequence1.compareTo(sequence2);
			}
			return result;
		}
	}

}
