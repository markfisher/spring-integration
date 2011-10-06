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

package org.springframework.integration.xmpp.support;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.packet.PacketExtension;

import org.springframework.integration.MessageHeaders;
import org.springframework.integration.mapping.AbstractHeaderMapper;
import org.springframework.integration.xmpp.XmppHeaders;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Default implementation of {@link XmppHeaderMapper}.
 *
 * @author Mark Fisher
 * @since 2.1
 */
public class DefaultXmppHeaderMapper extends AbstractHeaderMapper<Message> implements XmppHeaderMapper {

	public DefaultXmppHeaderMapper() {
		super(XmppHeaders.PREFIX);
	}

	@Override
	protected Map<String, Object> extractInboundStandardHeaders(Message source) {
		Map<String, Object> headers = new HashMap<String, Object>();
		Collection<PacketExtension> extensions = source.getExtensions();
		if (!CollectionUtils.isEmpty(extensions)) {
			for (PacketExtension extension : extensions) {
				String name = extension.getElementName();
				String namespace = extension.getNamespace();
				if (StringUtils.hasText(namespace)) {
					name = namespace + ":" + name;
				}
				headers.put(name, extension.toXML());
			}
		}
		String from = source.getFrom();
		if (StringUtils.hasText(from)) {
			headers.put(XmppHeaders.FROM, from);
		}
		String subject = source.getSubject();
		if (StringUtils.hasText(subject)) {
			headers.put(XmppHeaders.SUBJECT, subject);
		}
		String thread = source.getThread();
		if (StringUtils.hasText(thread)) {
			headers.put(XmppHeaders.THREAD, thread);
		}
		String to = source.getTo();
		if (StringUtils.hasText(to)) {
			headers.put(XmppHeaders.TO, to);
		}
		Message.Type type = source.getType();
		if (type != null) {
			headers.put(XmppHeaders.TYPE, type);
		}
		return headers;
	}

	@Override
	protected Map<String, Object> extractInboundUserDefinedHeaders(Message source) {
		Map<String, Object> headers = new HashMap<String, Object>();
		for (String propertyName : source.getPropertyNames()) {
			headers.put(propertyName, source.getProperty(propertyName));
		}
		return headers;
	}

	@Override
	protected void populateOutboundStandardHeaders(MessageHeaders headers, Message target) {
		String threadId = headers.get(XmppHeaders.THREAD, String.class);
		if (StringUtils.hasText(threadId)) {
			target.setThread(threadId);
		}
		String to = headers.get(XmppHeaders.TO, String.class);
		if (StringUtils.hasText(to)) {
			target.setTo(to);
		}
		String from = headers.get(XmppHeaders.FROM, String.class);
		if (StringUtils.hasText(from)) {
			target.setFrom(from);
		}
		String subject = headers.get(XmppHeaders.SUBJECT, String.class);
		if (StringUtils.hasText(subject)) {
			target.setSubject(subject);
		}
		Object typeHeader = headers.get(XmppHeaders.TYPE);
		if (typeHeader instanceof String) {
			try {
				typeHeader = Message.Type.valueOf((String) typeHeader);
			}
			catch (Exception e) {
				if (logger.isWarnEnabled()) {
					logger.warn("XMPP Type must be either a valid [Message.Type] " +
							"enum value or a String representation of such.");
				}
			}
		}
		if (typeHeader instanceof Message.Type) {
			target.setType((Message.Type) typeHeader);
		}
	}

	@Override
	protected void populateOutboundUserDefinedHeader(String headerName, Object headerValue, Message target) {
		target.setProperty(headerName, headerValue);
	}

}
