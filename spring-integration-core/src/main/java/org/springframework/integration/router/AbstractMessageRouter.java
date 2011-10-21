/*
 * Copyright 2002-2011 the original author or authors.
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

package org.springframework.integration.router;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.integration.Message;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.MessageDeliveryException;
import org.springframework.integration.MessagingException;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.channel.BeanFactoryChannelResolver;
import org.springframework.integration.support.channel.ChannelResolutionException;
import org.springframework.integration.support.channel.ChannelResolver;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Base class for all Message Routers.
 * 
 * @author Mark Fisher
 * @author Oleg Zhurakousky
 * @author Gunnar Hillert
 */
@ManagedResource
public abstract class AbstractMessageRouter extends AbstractMessageHandler {

	private volatile MessageChannel defaultOutputChannel;

	private volatile boolean ignoreSendFailures;

	private volatile boolean applySequence;

	private final MessagingTemplate messagingTemplate = new MessagingTemplate();

	private volatile String prefix;

	private volatile String suffix;

	private volatile ChannelResolver channelResolver;

	private volatile boolean resolutionRequired = true;

	private final Map<String, String> channelMappings = new ConcurrentHashMap<String, String>();


	/**
	 * Specify the {@link ChannelResolver} strategy to use.
	 * The default is a BeanFactoryChannelResolver.
	 */
	public void setChannelResolver(ChannelResolver channelResolver) {
		Assert.notNull(channelResolver, "'channelResolver' must not be null");
		this.channelResolver = channelResolver;
	}

	/**
	 * Specify a prefix to be added to each channel name prior to resolution.
	 */
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	/**
	 * Specify a suffix to be added to each channel name prior to resolution.
	 */
	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}

	/**
	 * Provide mappings from channel keys to channel names.
	 * Channel names will be resolved by the {@link ChannelResolver}.
	 */
	public void setChannelMappings(Map<String, String> channelMappings) {
		this.channelMappings.clear();
		this.channelMappings.putAll(channelMappings);
	}

	/**
	 * Returns an unmodifiable version of the channel mappings.
	 * This is intended for use by subclasses only.
	 */
	protected Map<String, String> getChannelMappings() {
		return Collections.unmodifiableMap(this.channelMappings);
	}

	/**
	 * Returns a mapped value for a given key, if present.
	 * This is intended for use by subclasses only.
	 */
	protected String getChannelMapping(String key) {
		return this.channelMappings.get(key);
	}

	/**
	 * Add a channel mapping from the provided key to channel name.
	 */
	@ManagedOperation
	public void setChannelMapping(String key, String channelName) {
		this.channelMappings.put(key, channelName);
	}

	/**
	 * Remove a channel mapping for the given key if present.
	 */
	@ManagedOperation
	public void removeChannelMapping(String key) {
		this.channelMappings.remove(key);
	}

	/**
	 * Set the default channel where Messages should be sent if channel resolution 
	 * fails to return any channels. If no default channel is provided and channel 
	 * resolution fails to return any channels, the router will throw an 
	 * {@link MessageDeliveryException}. 
	 * 
	 * If messages shall be ignored (dropped) instead, please provide a {@link NullChannel}. 
	 */
	public void setDefaultOutputChannel(MessageChannel defaultOutputChannel) {
		this.defaultOutputChannel = defaultOutputChannel;
	}

	/**
	 * Set the timeout for sending a message to the resolved channel. By default, there is no timeout, meaning the send
	 * will block indefinitely.
	 */
	public void setTimeout(long timeout) {
		this.messagingTemplate.setSendTimeout(timeout);
	}

	/**
	 * Specify whether this router should ignore any failure to resolve a channel name to
	 * an actual MessageChannel instance when delegating to the ChannelResolver strategy.
	 */
	public void setResolutionRequired(boolean resolutionRequired) {
		this.resolutionRequired = resolutionRequired;
	}

	/**
	 * Specify whether send failures for one or more of the recipients should be ignored. By default this is
	 * <code>false</code> meaning that an Exception will be thrown whenever a send fails. To override this and suppress
	 * Exceptions, set the value to <code>true</code>.
	 */
	public void setIgnoreSendFailures(boolean ignoreSendFailures) {
		this.ignoreSendFailures = ignoreSendFailures;
	}

	/**
	 * Specify whether to apply the sequence number and size headers to the messages prior to sending to the recipient
	 * channels. By default, this value is <code>false</code> meaning that sequence headers will <em>not</em> be
	 * applied. If planning to use an Aggregator downstream with the default correlation and completion strategies, you
	 * should set this flag to <code>true</code>.
	 */
	public void setApplySequence(boolean applySequence) {
		this.applySequence = applySequence;
	}

	@Override
	public String getComponentType() {
		return "router";
	}

	/**
	 * Provides {@link MessagingTemplate} access for subclasses.
	 */
	protected MessagingTemplate getMessagingTemplate() {
		return this.messagingTemplate;
	}

	@Override
	public void onInit() {
		BeanFactory beanFactory = this.getBeanFactory();
		if (this.channelResolver == null && beanFactory != null) {
			this.channelResolver = new BeanFactoryChannelResolver(beanFactory);
		}
	}

	protected ConversionService getRequiredConversionService() {
		if (this.getConversionService() == null) {
			this.setConversionService(ConversionServiceFactory.createDefaultConversionService());
		}
		return this.getConversionService();
	}

	/**
	 * Subclasses must implement this method to return the channel keys.
	 */
	protected abstract List<Object> getChannelIdentifiers(Message<?> message);


	@Override
	protected void handleMessageInternal(Message<?> message) {
		boolean sent = false;
		Collection<MessageChannel> results = this.determineTargetChannels(message);
		if (results != null) {
			int sequenceSize = results.size();
			int sequenceNumber = 1;
			for (MessageChannel channel : results) {
				final Message<?> messageToSend = (!this.applySequence) ? message : MessageBuilder.fromMessage(message)
						.pushSequenceDetails(message.getHeaders().getId(), sequenceNumber++, sequenceSize).build();
				if (channel != null) {
					try {
						this.messagingTemplate.send(channel, messageToSend);
						sent = true;
					}
					catch (MessagingException e) {
						if (!this.ignoreSendFailures) {
							throw e;
						}
						else if (this.logger.isDebugEnabled()) {
							this.logger.debug(e);
						}
					}
				}
			}
		}
		if (!sent) {
			if (this.defaultOutputChannel != null) {
				this.messagingTemplate.send(this.defaultOutputChannel, message);
			} else {
				throw new MessageDeliveryException(message,
						"no channel resolved by router and no default output channel defined");
			}
		}
	}

	private Collection<MessageChannel> determineTargetChannels(Message<?> message) {
		Collection<MessageChannel> channels = new ArrayList<MessageChannel>();
		Collection<Object> channelsReturned = this.getChannelIdentifiers(message);
		addToCollection(channels, channelsReturned, message);
		return channels;
	}

	private MessageChannel resolveChannelForName(String channelName, Message<?> message) {
		if (this.channelResolver == null) {
			this.onInit();
		}
		Assert.state(this.channelResolver != null,
				"unable to resolve channel names, no ChannelResolver available");
		MessageChannel channel = null;
		try {
			channel = this.channelResolver.resolveChannelName(channelName);
		}
		catch (ChannelResolutionException e) {
			if (this.resolutionRequired) {
				throw new MessagingException(message,
						"failed to resolve channel name '" + channelName + "'", e);
			}
		}
		if (channel == null && this.resolutionRequired) {
			throw new MessagingException(message,
					"failed to resolve channel name '" + channelName + "'");
		}
		return channel;
	}

	private void addChannelFromString(Collection<MessageChannel> channels, String channelKey, Message<?> message) {
		if (channelKey.indexOf(',') != -1) {
			for (String name : StringUtils.tokenizeToStringArray(channelKey, ",")) {
				addChannelFromString(channels, name, message);
			}
			return;
		}

		// if the channelMappings contains a mapping, we'll use the mapped value
		// otherwise, the String-based channelKey itself will be used as the channel name
		String channelName = channelKey;
		if (!CollectionUtils.isEmpty(channelMappings) && channelMappings.containsKey(channelKey)) {
			channelName = channelMappings.get(channelKey);
		}
		if (this.prefix != null) {
			channelName = this.prefix + channelName;
		}
		if (this.suffix != null) {
			channelName = channelName + this.suffix;
		}
		MessageChannel channel = resolveChannelForName(channelName, message);
		if (channel != null) {
			channels.add(channel);
		}
	}

	private void addToCollection(Collection<MessageChannel> channels, Collection<?> channelIndicators, Message<?> message) {
		if (channelIndicators == null) {
			return;
		}
		for (Object channelIndicator : channelIndicators) {
			if (channelIndicator == null) {
				continue;
			}
			else if (channelIndicator instanceof MessageChannel) {
				channels.add((MessageChannel) channelIndicator);
			}
			else if (channelIndicator instanceof MessageChannel[]) {
				channels.addAll(Arrays.asList((MessageChannel[]) channelIndicator));
			}
			else if (channelIndicator instanceof String) {
				addChannelFromString(channels, (String) channelIndicator, message);
			}
			else if (channelIndicator instanceof String[]) {
				for (String indicatorName : (String[]) channelIndicator) {
					addChannelFromString(channels, indicatorName, message);
				}
			}
			else if (channelIndicator instanceof Collection) {
				addToCollection(channels, (Collection<?>) channelIndicator, message);
			}
			else if (this.getRequiredConversionService().canConvert(channelIndicator.getClass(), String.class)) {
				addChannelFromString(channels,
						this.getConversionService().convert(channelIndicator, String.class), message);
			}
			else {
				throw new MessagingException(
						"unsupported return type for router [" + channelIndicator.getClass() + "]");
			}
		}
	}

}
