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

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.integration.MessageHeaders;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.PatternMatchUtils;
import org.springframework.util.StringUtils;

/**
 * Abstract base class for HeaderMapper implementations.
 *
 * @author Mark Fisher
 * @since 2.1
 */
public abstract class RefactoredAbstractHeaderMapper<T> {//implements HeaderMapper<T> {

//	public static final String ALL_REQUEST_HEADERS_PATTERN = "ALL_REQUEST_HEADERS";
//
//	public static final String ALL_REPLY_HEADERS_PATTERN = "ALL_REPLY_HEADERS";
//
//	private static final String[] TRANSIENT_HEADER_NAMES = new String[] {
//		MessageHeaders.ID,
//		MessageHeaders.ERROR_CHANNEL,
//		MessageHeaders.REPLY_CHANNEL,
//		MessageHeaders.TIMESTAMP
//	};
//
//
//	protected final Log logger = LogFactory.getLog(this.getClass());
//
//	private final String standardHeaderPrefix;
//
//	private volatile String[] inboundHeaderNames = new String[0];
//
//	private volatile String[] outboundHeaderNames = new String[0];
//
//	private volatile String inboundPrefix = "";
//
//	private volatile String outboundPrefix = "";
//
//
//	protected AbstractHeaderMapper(String standardHeaderPrefix) {
//		this.standardHeaderPrefix = standardHeaderPrefix;
//	}
//
//
//	/**
//	 * Provide the header names that should be mapped from an inbound request (for inbound adapters)
//	 * or response (for outbound adapters) to a Spring Integration Message's headers.
//	 * The values can also contain simple wildcard patterns (e.g. "foo*" or "*foo") to be matched.
//	 * <p>
//	 * This will match the header name directly or, for non-standard headers, it will match
//	 * the header name prefixed with the value specified by 
//	 * {@link #setInboundPrefix(String)}.
//	 */
//	public void setInboundHeaderNames(String[] inboundHeaderNames) {
//		this.inboundHeaderNames = (inboundHeaderNames != null) ? inboundHeaderNames : new String[0];
//	}
//
//	/**
//	 * Provide the header names that should be mapped to an outbound request (for outbound adapters)
//	 * or outbound response (for inbound adapters) from a Spring Integration Message's headers.
//	 * The values can also contain simple wildcard patterns (e.g. "foo*" or "*foo") to be matched.
//	 * <p>
//	 * Any non-standard headers will be prefixed with the value specified by 
//	 * {@link #setOutboundPrefix(String)}.
//	 */
//	public void setOutboundHeaderNames(String[] outboundHeaderNames) {
//		this.outboundHeaderNames = (outboundHeaderNames != null) ? outboundHeaderNames : new String[0];
//	}
//
//	/**
//	 * Specify a prefix to be prepended to the integration message header name for any
//	 * user-defined header that is being mapped into the MessageHeaders.
//	 * The Default is an empty string (no prefix).
//	 * <p/>
//	 * This does not affect the standard properties for the particular protocol, such as
//	 * contentType for AMQP, etc. The header names used for mapping such properties are
//	 * defined in a corresponding Headers class as constants (e.g. AmqpHeaders).
//	 */
//	public void setInboundPrefix(String inboundPrefix) {
//		this.inboundPrefix = (inboundPrefix != null) ? inboundPrefix : "";
//	}
//
//	/**
//	 * Specify a prefix to be prepended to the header name for any integration
//	 * message header that is being mapped into the outbound request.
//	 * The Default is an empty string (no prefix).
//	 * <p/>
//	 * This does not affect the standard properties for the particular protocol, such as
//	 * contentType for AMQP, etc. The header names used for mapping such properties are
//	 * defined in a corresponding Headers class as constants (e.g. AmqpHeaders).
//	 */
//	public void setOutboundPrefix(String outboundPrefix) {
//		this.outboundPrefix = (outboundPrefix != null) ? outboundPrefix : "";
//	}
//
//	/**
//	 * Maps headers from a Spring Integration MessageHeaders instance to the target instance.
//	 */
//	public final void fromHeaders(MessageHeaders headers, T target) {
//		try {
//			this.populateOutboundStandardHeaders(headers, target);
//			this.populateOutboundUserDefinedHeaders(headers, target);
//		}
//		catch (Exception e) {
//			if (logger.isWarnEnabled()) {
//				logger.warn("error occurred while mapping from MessageHeaders", e);
//			}
//		}
//	}
//
//	private void populateOutboundHeaders(MessageHeaders headers, T target) {
//		Set<String> headerNames = headers.keySet();
//		for (String headerName : headerNames) {
//			if (this.shouldMapOutboundHeader(headerName)) {
//				Object value = headers.get(headerName);
//				if (value != null) {
//					try {
//						String key = this.prefixHeaderNameIfNecessary(this.outboundPrefix, headerName);
//						this.populateOutboundUserDefinedHeader(key, value, target);
//					}
//					catch (Exception e) {
//						if (logger.isWarnEnabled()) {
//							logger.warn("failed to map from Message header '" + headerName + "' to target", e);
//						}
//					}
//				}
//			}
//		}
//	}
//
//	private void populateOutboundStandardHeaders(MessageHeaders headers, T target) {
//		Set<String> headerNames = headers.keySet();
//		for (String headerName : headerNames) {
//			if (this.shouldMapOutboundHeader(headerName)) {
//				Object value = headers.get(headerName);
//				if (value != null) {
//					try {
//						String key = this.prefixHeaderNameIfNecessary(this.outboundPrefix, headerName);
//						this.populateOutboundStandardHeader(key, value, target);
//					}
//					catch (Exception e) {
//						if (logger.isWarnEnabled()) {
//							logger.warn("failed to map from Message header '" + headerName + "' to target", e);
//						}
//					}
//				}
//			}
//		}
//	}
//
//	/**
//	 * Maps headers from a source instance to the MessageHeaders of a
//	 * Spring Integration Message.
//	 */
//	public final <V> Map<String, V> toHeaders(T source) {
//		if (logger.isDebugEnabled()) {
//			logger.debug(MessageFormat.format("inboundHeaderNames={0}", CollectionUtils.arrayToList(inboundHeaderNames)));
//		}
//		Map<String, V> headers = new HashMap<String, V>();
//		Map<String, Object> standardHeaders = this.extractInboundStandardHeaders(source);
//		this.copyHeaders(this.standardHeaderPrefix, standardHeaders, headers);
//		Map<String, Object> userDefinedHeaders = this.extractInboundUserDefinedHeaders(source);
//		this.copyHeaders(this.inboundPrefix, userDefinedHeaders, headers);
//		return headers;
//	}
//
//	private <V> void copyHeaders(String prefix, Map<String, Object> source, Map<String, V> target) {
//		if (!CollectionUtils.isEmpty(source)) {
//			for (Map.Entry<String, Object> entry : source.entrySet()) {
//				try {
//					String headerName = this.prefixHeaderNameIfNecessary(prefix, entry.getKey());
//					if (!ObjectUtils.containsElement(TRANSIENT_HEADER_NAMES, headerName)) {
//						target.put(headerName, (V) entry.getValue());
//					}
//				}
//				catch (Exception e) {
//					if (logger.isWarnEnabled()) {
//						logger.warn("error occurred while mapping header '"
//								+ entry.getKey() + "' to Message header", e);
//					}
//				}
//			}
//		}
//	}
//
//	/*private boolean shouldMapOutboundHeader(String headerName) {
//		return StringUtils.hasText(headerName)
//				&& (standardHeaderPrefix == null || !headerName.startsWith(this.standardHeaderPrefix))
//				&& !ObjectUtils.containsElement(TRANSIENT_HEADER_NAMES, headerName);
//	}*/
//
//	private boolean shouldMapOutboundHeader(String headerName) {
//		return StringUtils.hasText(headerName)
//				&& !ObjectUtils.containsElement(TRANSIENT_HEADER_NAMES, headerName)
//				&& this.shouldMapHeader(headerName, this.outboundHeaderNames);
//	}
//
//	private boolean shouldMapInboundHeader(String headerName) {
//		return StringUtils.hasText(headerName)
//				&& !ObjectUtils.containsElement(TRANSIENT_HEADER_NAMES, headerName)
//				&& this.shouldMapHeader(headerName, this.inboundHeaderNames);
//	}
//
//	private boolean shouldMapHeader(String headerName, String[] patterns) {
//		if (patterns != null && patterns.length > 0) {
//			for (String pattern : patterns) {
//				if (PatternMatchUtils.simpleMatch(pattern.toLowerCase(), headerName.toLowerCase())) {
//					if (logger.isDebugEnabled()) {
//						logger.debug(MessageFormat.format("headerName=[{0}] WILL be mapped, matched pattern={1}", headerName, pattern));
//					}
//					return true;
//				}
//				else if (ALL_REQUEST_HEADERS_PATTERN.equals(pattern)
//						&& this.containsElementIgnoreCase(this.getStandardRequestHeaderNames(), headerName)) {
//					if (logger.isDebugEnabled()) {
//						logger.debug(MessageFormat.format("headerName=[{0}] WILL be mapped, matched pattern={1}", headerName, pattern));
//					}
//					return true;
//				}
//				else if (ALL_REPLY_HEADERS_PATTERN.equals(pattern)
//						&& this.containsElementIgnoreCase(this.getStandardReplyHeaderNames(), headerName)) {
//					if (logger.isDebugEnabled()) {
//						logger.debug(MessageFormat.format("headerName=[{0}] WILL be mapped, matched pattern={1}", headerName, pattern));
//					}
//					return true;
//				}
//			}
//		}
//		if (logger.isDebugEnabled()) {
//			logger.debug(MessageFormat.format("headerName=[{0}] WILL NOT be mapped", headerName));
//		}
//		return false;
//	}
//
//	protected <V> V getHeaderIfAvailable(MessageHeaders headers, String name, Class<V> type) {
//		try {
//			return headers.get(name, type);
//		}
//		catch (IllegalArgumentException e) {
//			if (logger.isWarnEnabled()) {
//				logger.warn("skipping header '" + name + "' since it is not of expected type [" + type + "]", e);
//			}
//			return null;
//		}
//	}
//
//	/**
//	 * Adds the outbound prefix if necessary.
//	 */
//	/*private String fromHeaderName(String headerName) {
//		String propertyName = headerName;
//		if (StringUtils.hasText(this.outboundPrefix) && !propertyName.startsWith(this.outboundPrefix)) {
//			propertyName = this.outboundPrefix + headerName;
//		}
//		return propertyName;
//	}*/
//
//	/**
//	 * Adds the inbound prefix if necessary.
//	 */
//	private String prefixHeaderNameIfNecessary(String prefix, String propertyName) {
//		String headerName = propertyName;
//		if (StringUtils.hasText(prefix) && !headerName.startsWith(prefix)) {
//			headerName = prefix + propertyName;
//		}
//		return headerName;
//	}
//
//	protected abstract String[] getStandardRequestHeaderNames();
//
//	protected abstract String[] getStandardReplyHeaderNames();
//
//	protected abstract Map<String, Object> extractInboundStandardHeaders(T source);
//
//	protected abstract Map<String, Object> extractInboundUserDefinedHeaders(T source);
//
//	protected abstract void populateOutboundStandardHeaders(MessageHeaders headers, T target);
//
//	protected abstract void populateOutboundUserDefinedHeader(String headerName, Object headerValue, T target);

}
