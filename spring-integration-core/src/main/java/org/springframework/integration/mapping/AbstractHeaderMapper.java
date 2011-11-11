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

package org.springframework.integration.mapping;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.integration.MessageHeaders;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.FieldCallback;
import org.springframework.util.StringUtils;

/**
 * Abstract base class for HeaderMapper implementations.
 *
 * @author Mark Fisher
 * @since 2.1
 */
public abstract class AbstractHeaderMapper<T> implements HeaderMapper<T> {

	private static final String[] TRANSIENT_HEADER_NAMES = new String[] {
		MessageHeaders.ID,
		MessageHeaders.ERROR_CHANNEL,
		MessageHeaders.REPLY_CHANNEL,
		MessageHeaders.TIMESTAMP
	};


	protected final Log logger = LogFactory.getLog(this.getClass());

	private final String standardHeaderPrefix;

	private final List<String> standardRequestHeaderNames = new ArrayList<String>();

	private final List<String> standardReplyHeaderNames = new ArrayList<String>();

	private volatile String[] inboundHeaderNames = new String[0];

	private volatile String[] outboundHeaderNames = new String[0];

	private volatile String inboundPrefix = "";

	private volatile String outboundPrefix = "";


	protected AbstractHeaderMapper(String standardHeaderPrefix, List<String> standardInboundHeaderNames, List<String> standardOutboundHeaderNames) {
		this.standardHeaderPrefix = standardHeaderPrefix;
		this.standardRequestHeaderNames.addAll(standardInboundHeaderNames);
		this.standardReplyHeaderNames.addAll(standardOutboundHeaderNames);
	}

	protected AbstractHeaderMapper(Class<?> headersClass, boolean outbound) {
		final StringBuilder prefixBuilder = new StringBuilder();
		final List<String> headerNames = new ArrayList<String>();
		try {
			ReflectionUtils.doWithFields(headersClass, new FieldCallback() {
				public void doWith(Field f) throws IllegalArgumentException, IllegalAccessException {
					// TODO: wrap each get() with a try/catch and ignore exceptions (delegate this call)
					if ("PREFIX".equals(f.getName())) {
						Object value = f.get(null);
						if (value instanceof String) {
							prefixBuilder.append((String) value);
						}
					}
					else {
						Object value = f.get(null);
						if (value instanceof String) {
							headerNames.add((String) value);
						}
					}
				}
			});
		}
		catch (Exception e) {
			// ignore
		}
		this.standardHeaderPrefix = prefixBuilder.toString();
		this.standardRequestHeaderNames.addAll(headerNames);
		this.standardReplyHeaderNames.addAll(headerNames);
		if (outbound) {
			this.outboundHeaderNames = this.standardRequestHeaderNames.toArray(new String[0]);
			this.inboundHeaderNames = this.standardReplyHeaderNames.toArray(new String[0]);
		}
		else {
			this.inboundHeaderNames = this.standardRequestHeaderNames.toArray(new String[0]);
			this.outboundHeaderNames = this.standardReplyHeaderNames.toArray(new String[0]);			
		}
	}

	/**
	 * Provide the header names that should be mapped from a request (for inbound adapters)
	 * or response (for outbound adapters) to a Spring Integration Message's headers.
	 * The values can also contain simple wildcard patterns (e.g. "foo*" or "*foo") to be matched.
	 * <p>
	 * This will match the header name directly or, for non-standard headers, it will match
	 * the header name prefixed with the value specified by {@link #setInboundPrefix(String)}.
	 */
	public void setInboundHeaderNames(String[] inboundHeaderNames) {
		this.inboundHeaderNames = (inboundHeaderNames != null) ? inboundHeaderNames : new String[0];
	}

	/**
	 * Provide the header names that should be mapped to a request (for outbound adapters)
	 * or response (for inbound adapters) from a Spring Integration Message's headers.
	 * The values can also contain simple wildcard patterns (e.g. "foo*" or "*foo") to be matched.
	 * <p>
	 * Any non-standard headers will be prefixed with the value specified by {@link #setOutboundPrefix(String)}.
	 */
	public void setOutboundHeaderNames(String[] outboundHeaderNames) {
		this.outboundHeaderNames = (outboundHeaderNames != null) ? outboundHeaderNames : new String[0];
	}

	/**
	 * Specify a prefix to be prepended to the integration message header name for any
	 * user-defined header that is being mapped into the MessageHeaders.
	 * The Default is an empty string (no prefix).
	 * <p/>
	 * This does not affect the standard properties for the particular protocol, such as
	 * contentType for AMQP, etc. The header names used for mapping such properties are
	 * defined in a corresponding Headers class as constants (e.g. AmqpHeaders).
	 */
	public void setInboundPrefix(String inboundPrefix) {
		this.inboundPrefix = (inboundPrefix != null) ? inboundPrefix : "";
	}

	/**
	 * Specify a prefix to be prepended to the header name for any integration
	 * message header that is being mapped into the outbound request.
	 * The Default is an empty string (no prefix).
	 * <p/>
	 * This does not affect the standard properties for the particular protocol, such as
	 * contentType for AMQP, etc. The header names used for mapping such properties are
	 * defined in a corresponding Headers class as constants (e.g. AmqpHeaders).
	 */
	public void setOutboundPrefix(String outboundPrefix) {
		this.outboundPrefix = (outboundPrefix != null) ? outboundPrefix : "";
	}

	/**
	 * Maps headers from a Spring Integration MessageHeaders instance to the target instance.
	 */
	public void fromHeaders(MessageHeaders headers, T target) {
		try {
			Map<String, Object> subset = new HashMap<String, Object>();
			for (String headerName : headers.keySet()) {
				if (this.getStandardOutboundHeaderNames().contains(headerName) && this.shouldMapOutboundHeader(headerName)) {
					subset.put(headerName, headers.get(headerName));
				}
			}
			this.populateStandardOutboundHeaders(new MessageHeaders(subset), target);
			this.populateUserDefinedOutboundHeaders(headers, target);
		}
		catch (Exception e) {
			if (logger.isWarnEnabled()) {
				logger.warn("error occurred while mapping from MessageHeaders", e);
			}
		}
	}

	private void populateUserDefinedOutboundHeaders(MessageHeaders headers, T target) {
		Set<String> headerNames = headers.keySet();
		for (String headerName : headerNames) {
			if (this.shouldMapOutboundHeader(headerName)) {
				Object value = headers.get(headerName);
				if (value != null) {
					try {
						String key = this.prefixHeaderNameIfNecessary(this.outboundPrefix, headerName);
						this.populateUserDefinedOutboundHeader(key, value, target);
					}
					catch (Exception e) {
						if (logger.isWarnEnabled()) {
							logger.warn("failed to map from Message header '" + headerName + "' to target", e);
						}
					}
				}
			}
		}
	}

	/**
	 * Maps headers from a source instance to the MessageHeaders of a
	 * Spring Integration Message.
	 */
	public <V> Map<String, V> toHeaders(T source) {
		Map<String, V> headers = new HashMap<String, V>();
		Map<String, Object> standardHeaders = this.extractStandardInboundHeaders(source);
		this.copyInboundHeaders(this.standardHeaderPrefix, standardHeaders, headers);
		Map<String, Object> userDefinedHeaders = this.extractUserDefinedInboundHeaders(source);
		this.copyInboundHeaders(this.inboundPrefix, userDefinedHeaders, headers);
		return headers;
	}

	private <V> void copyInboundHeaders(String prefix, Map<String, Object> source, Map<String, V> target) {
		if (!CollectionUtils.isEmpty(source)) {
			for (Map.Entry<String, Object> entry : source.entrySet()) {
				try {
					String headerName = this.prefixHeaderNameIfNecessary(prefix, entry.getKey());
					if (this.shouldMapInboundHeader(headerName)) {
						target.put(headerName, (V) entry.getValue());
					}
				}
				catch (Exception e) {
					if (logger.isWarnEnabled()) {
						logger.warn("error occurred while mapping header '"
								+ entry.getKey() + "' to Message header", e);
					}
				}
			}
		}
	}

	private boolean shouldMapInboundHeader(String headerName) {
		return StringUtils.hasText(headerName)
				&& ObjectUtils.containsElement(this.inboundHeaderNames, headerName)
				&& !ObjectUtils.containsElement(TRANSIENT_HEADER_NAMES, headerName);
	}

	private boolean shouldMapOutboundHeader(String headerName) {
		return StringUtils.hasText(headerName)
				&& ObjectUtils.containsElement(this.outboundHeaderNames, headerName)
				&& !ObjectUtils.containsElement(TRANSIENT_HEADER_NAMES, headerName);
	}

	protected <V> V getHeaderIfAvailable(MessageHeaders headers, String name, Class<V> type) {
		try {
			return headers.get(name, type);
		}
		catch (IllegalArgumentException e) {
			if (logger.isWarnEnabled()) {
				logger.warn("skipping header '" + name + "' since it is not of expected type [" + type + "]", e);
			}
			return null;
		}
	}

	/**
	 * Adds the outbound prefix if necessary.
	 */
	/*private String fromHeaderName(String headerName) {
		String propertyName = headerName;
		if (StringUtils.hasText(this.outboundPrefix) && !propertyName.startsWith(this.outboundPrefix)) {
			propertyName = this.outboundPrefix + headerName;
		}
		return propertyName;
	}*/

	/**
	 * Adds the inbound prefix if necessary.
	 */
	private String prefixHeaderNameIfNecessary(String prefix, String propertyName) {
		String headerName = propertyName;
		if (StringUtils.hasText(prefix) && !headerName.startsWith(prefix)) {
			headerName = prefix + propertyName;
		}
		return headerName;
	}

	/**
	 * By default this returns all header names discovered on the underlying Headers class.
	 * Subclasses may override. For example, a subset of headers may be used for inbound.
	 */
	protected List<String> getStandardInboundHeaderNames() {
		return Collections.unmodifiableList(this.standardRequestHeaderNames);
	}

	/**
	 * By default this returns all header names discovered on the underlying Headers class.
	 * Subclasses may override. For example, a subset of headers may be used for outbound.
	 */
	protected List<String> getStandardOutboundHeaderNames() {
		return Collections.unmodifiableList(this.standardReplyHeaderNames);
	}

	protected abstract Map<String, Object> extractStandardInboundHeaders(T source);

	protected abstract Map<String, Object> extractUserDefinedInboundHeaders(T source);

	protected abstract void populateStandardOutboundHeaders(MessageHeaders headers, T target);
	//protected abstract void populateOutboundStandardHeader(String headerName, Object headerValue, T target);

	protected abstract void populateUserDefinedOutboundHeader(String headerName, Object headerValue, T target);

}
