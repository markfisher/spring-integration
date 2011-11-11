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
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.integration.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.PatternMatchUtils;
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

	public static final String STANDARD_REQUEST_HEADER_NAME_PATTERN = "STANDARD_REQUEST_HEADERS";

	public static final String STANDARD_REPLY_HEADER_NAME_PATTERN = "STANDARD_REPLY_HEADERS";

	private static final String[] TRANSIENT_HEADER_NAMES = new String[] {
		MessageHeaders.ID,
		MessageHeaders.ERROR_CHANNEL,
		MessageHeaders.REPLY_CHANNEL,
		MessageHeaders.TIMESTAMP
	};


	protected final Log logger = LogFactory.getLog(this.getClass());

	private final boolean outbound;

	private final String standardHeaderPrefix;

	private volatile String userDefinedHeaderPrefix = "";

	private final List<String> standardRequestHeaderNames = new ArrayList<String>();

	private final List<String> standardReplyHeaderNames = new ArrayList<String>();

	private volatile String[] inboundHeaderNames = new String[0];

	private volatile String[] outboundHeaderNames = new String[0];


	protected AbstractHeaderMapper(Class<?> headersClass, boolean outbound) {
		Assert.notNull(headersClass, "headersClass must not be null");
		this.outbound = outbound;
		final StringBuilder prefixBuilder = new StringBuilder();
		final List<String> headerNames = new ArrayList<String>();
		this.introspectHeaderFields(headersClass, prefixBuilder, headerNames);
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


	private void introspectHeaderFields(Class<?> headersClass, final StringBuilder prefixBuilder, final List<String> headerNames) {
		ReflectionUtils.doWithFields(headersClass, new FieldCallback() {
			public void doWith(Field f) throws IllegalArgumentException, IllegalAccessException {
				try {
					String fieldName = f.getName();
					Object fieldValue = f.get(null);
					if (fieldValue instanceof String) {
						if ("PREFIX".equals(fieldName)) {
							prefixBuilder.append((String) fieldValue);
						}
						else {
							headerNames.add((String) fieldValue);
						}
					}
				}
				catch (Exception e) {
					// ignore
				}
			}
		});
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
	 * Specify a prefix to be prepended to the header name for any integration
	 * message header that is being mapped to or from a user-defined value.
	 * <p/>
	 * This does not affect the standard properties for the particular protocol, such as
	 * contentType for AMQP, etc. The header names used for mapping such properties are
	 * defined in a corresponding Headers class as constants (e.g. AmqpHeaders).
	 */
	public void setUserDefinedHeaderPrefix(String userDefinedHeaderPrefix) {
		this.userDefinedHeaderPrefix = (userDefinedHeaderPrefix != null) ? userDefinedHeaderPrefix : "";
	}

	/**
	 * Maps headers from a Spring Integration MessageHeaders instance to the target instance.
	 */
	public void fromHeaders(MessageHeaders headers, T target) {
		try {
			Map<String, Object> subset = new HashMap<String, Object>();
			for (String headerName : headers.keySet()) {
				boolean shouldMap = this.outbound
						? this.shouldMapOutboundHeader(headerName)
						: this.shouldMapInboundHeader(headerName);
				if (shouldMap) {
					subset.put(headerName, headers.get(headerName));
				}
			}
			this.populateStandardHeaders(subset, target);
			this.populateUserDefinedHeaders(subset, target);
		}
		catch (Exception e) {
			if (logger.isWarnEnabled()) {
				logger.warn("error occurred while mapping from MessageHeaders", e);
			}
		}
	}

	private void populateUserDefinedHeaders(Map<String, Object> headers, T target) {
		for (String headerName : headers.keySet()) {
			Object value = headers.get(headerName);
			if (value != null) {
				try {
					String key = this.addPrefixIfNecessary(this.userDefinedHeaderPrefix, headerName);
					this.populateUserDefinedHeader(key, value, target);
				}
				catch (Exception e) {
					if (logger.isWarnEnabled()) {
						logger.warn("failed to map from Message header '" + headerName + "' to target", e);
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
		Map<String, Object> standardHeaders = this.extractStandardHeaders(source);
		this.copyHeaders(this.standardHeaderPrefix, standardHeaders, headers);
		Map<String, Object> userDefinedHeaders = this.extractUserDefinedHeaders(source);
		this.copyHeaders(this.userDefinedHeaderPrefix, userDefinedHeaders, headers);
		return headers;
	}

	private <V> void copyHeaders(String prefix, Map<String, Object> source, Map<String, V> target) {
		if (!CollectionUtils.isEmpty(source)) {
			for (Map.Entry<String, Object> entry : source.entrySet()) {
				try {
					String headerName = this.addPrefixIfNecessary(prefix, entry.getKey());
					boolean shouldMap = this.outbound
							? this.shouldMapOutboundHeader(headerName)
							: this.shouldMapInboundHeader(headerName);
					if (shouldMap) {
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
		return this.shouldMapHeader(headerName, this.inboundHeaderNames);
	}

	private boolean shouldMapOutboundHeader(String headerName) {
		return this.shouldMapHeader(headerName, this.outboundHeaderNames);
	}

	private boolean shouldMapHeader(String headerName, String[] patterns) {
		if (!StringUtils.hasText(headerName)
				|| ObjectUtils.containsElement(TRANSIENT_HEADER_NAMES, headerName)) {
			return false;
		}
		if (patterns != null && patterns.length > 0) {
			for (String pattern : patterns) {
				if (PatternMatchUtils.simpleMatch(pattern.toLowerCase(), headerName.toLowerCase())) {
					if (logger.isDebugEnabled()) {
						logger.debug(MessageFormat.format("headerName=[{0}] WILL be mapped, matched pattern={1}", headerName, pattern));
					}
					return true;
				}
				else if (STANDARD_REQUEST_HEADER_NAME_PATTERN.equals(pattern)
						&& this.containsElementIgnoreCase(this.standardRequestHeaderNames, headerName)) {
					if (logger.isDebugEnabled()) {
						logger.debug(MessageFormat.format("headerName=[{0}] WILL be mapped, matched pattern={1}", headerName, pattern));
					}
					return true;
				}
				else if (STANDARD_REPLY_HEADER_NAME_PATTERN.equals(pattern)
						&& this.containsElementIgnoreCase(this.standardReplyHeaderNames, headerName)) {
					if (logger.isDebugEnabled()) {
						logger.debug(MessageFormat.format("headerName=[{0}] WILL be mapped, matched pattern={1}", headerName, pattern));
					}
					return true;
				}
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug(MessageFormat.format("headerName=[{0}] WILL NOT be mapped", headerName));
		}
		return false;
	}

	private boolean containsElementIgnoreCase(List<String> headerNames, String name) {
		for (String headerName : headerNames) {
			if (headerName.equalsIgnoreCase(name)){
				return true;
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	protected <V> V getHeaderIfAvailable(Map<String, Object> headers, String name, Class<V> type) {
		Object value = headers.get(name);
		if (value == null) {
			return null;
		}
		if (!type.isAssignableFrom(value.getClass())) {
			if (logger.isWarnEnabled()) {
				logger.warn("skipping header '" + name + "' since it is not of expected type [" + type + "]");
			}
		}
		return (V) value;
	}

	/**
	 * Adds the outbound or inbound prefix if necessary.
	 */
	private String addPrefixIfNecessary(String prefix, String propertyName) {
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

	protected abstract Map<String, Object> extractStandardHeaders(T source);

	protected abstract Map<String, Object> extractUserDefinedHeaders(T source);

	protected abstract void populateStandardHeaders(Map<String, Object> headers, T target);

	protected abstract void populateUserDefinedHeader(String headerName, Object headerValue, T target);

}
