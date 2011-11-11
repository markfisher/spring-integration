/*
 * Copyright 2002-2011 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.integration.amqp.outbound;

import java.util.Map;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.Message;
import org.springframework.integration.amqp.support.AmqpHeaderMapper;
import org.springframework.integration.amqp.support.DefaultAmqpHeaderMapper;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.integration.handler.ExpressionEvaluatingMessageProcessor;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.util.Assert;

/**
 * Adapter that converts and sends Messages to an AMQP Exchange.
 * 
 * @author Mark Fisher
 * @since 2.1
 */
public class AmqpOutboundEndpoint extends AbstractReplyProducingMessageHandler {

	private static final ExpressionParser expressionParser = new SpelExpressionParser(new SpelParserConfiguration(true, true));


	private final AmqpTemplate amqpTemplate;

	private volatile boolean expectReply;

	private volatile String exchangeName = "";

	private volatile String routingKey = "";

	private volatile String exchangeNameExpression;

	private volatile String routingKeyExpression;

	private volatile ExpressionEvaluatingMessageProcessor<String> routingKeyGenerator;

	private volatile ExpressionEvaluatingMessageProcessor<String> exchangeNameGenerator;

	private volatile AmqpHeaderMapper headerMapper = new DefaultAmqpHeaderMapper(true);


	@Override
	protected void onInit() {
		super.onInit();
		Assert.state(exchangeNameExpression == null || "".equals(exchangeName),
				"Either an exchangeName or an exchangeNameExpression can be provided, but not both");
		if (exchangeNameExpression != null) {
			Expression expression = expressionParser.parseExpression(this.exchangeNameExpression);
			this.exchangeNameGenerator = new ExpressionEvaluatingMessageProcessor<String>(expression, String.class);
		}
		Assert.state(routingKeyExpression == null || "".equals(routingKey),
				"Either a routingKey or a routingKeyExpression can be provided, but not both");
		if (routingKeyExpression != null) {
			Expression expression = expressionParser.parseExpression(this.routingKeyExpression);
			this.routingKeyGenerator = new ExpressionEvaluatingMessageProcessor<String>(expression, String.class);
		}
	}

	public AmqpOutboundEndpoint(AmqpTemplate amqpTemplate) {
		Assert.notNull(amqpTemplate, "AmqpTemplate must not be null");
		this.amqpTemplate = amqpTemplate;
	}

	public void setExchangeName(String exchangeName) {
		this.exchangeName = exchangeName;
	}

	public void setExchangeNameExpression(String exchangeNameExpression) {
		this.exchangeNameExpression = exchangeNameExpression;
	}

	public void setRoutingKey(String routingKey) {
		this.routingKey = routingKey;
	}

	public void setRoutingKeyExpression(String routingKeyExpression) {
		this.routingKeyExpression = routingKeyExpression;
	}

	public void setExpectReply(boolean expectReply) {
		this.expectReply = expectReply;
	}

	@Override
	public String getComponentType() {
		return expectReply ? "amqp:outbound-channel-adapter" : "amqp:outbound-gateway";
	}

	@Override
	protected Object handleRequestMessage(Message<?> requestMessage) {
		String exchangeName = this.exchangeName;
		String routingKey = this.routingKey;
		if (this.exchangeNameGenerator != null) {
			exchangeName = this.exchangeNameGenerator.processMessage(requestMessage);
		}
		if (this.routingKeyGenerator != null) {
			routingKey = this.routingKeyGenerator.processMessage(requestMessage);
		}
		if (this.expectReply) {
			return this.sendAndReceive(exchangeName, routingKey, requestMessage);
		}
		else {
			this.send(exchangeName, routingKey, requestMessage);
			return null;
		}
	}

	private void send(String exchangeName, String routingKey, final Message<?> requestMessage) {
		this.amqpTemplate.convertAndSend(exchangeName, routingKey, requestMessage.getPayload(),
				new MessagePostProcessor() {
					public org.springframework.amqp.core.Message postProcessMessage(
							org.springframework.amqp.core.Message message) throws AmqpException {
						headerMapper.fromHeaders(requestMessage.getHeaders(), message.getMessageProperties());
						return message;
					}
				});
	}

	private Message<?> sendAndReceive(String exchangeName, String routingKey, Message<?> requestMessage) {
		// TODO: add a convertSendAndReceive method that accepts a MessagePostProcessor so we can map headers?
		Assert.isTrue(amqpTemplate instanceof RabbitTemplate, "RabbitTemplate implementation is required for send and receive");
		MessageConverter converter = ((RabbitTemplate) this.amqpTemplate).getMessageConverter();
		MessageProperties amqpMessageProperties = new MessageProperties();
		this.headerMapper.fromHeaders(requestMessage.getHeaders(), amqpMessageProperties);
		org.springframework.amqp.core.Message amqpMessage = converter.toMessage(requestMessage.getPayload(), amqpMessageProperties);
		org.springframework.amqp.core.Message amqpReplyMessage = this.amqpTemplate.sendAndReceive(exchangeName, routingKey, amqpMessage);
		if (amqpReplyMessage == null) {
			return null;
		}
		Object replyObject = converter.fromMessage(amqpReplyMessage);
		MessageBuilder<?> builder = (replyObject instanceof Message)
				? MessageBuilder.fromMessage((Message<?>) replyObject)
				: MessageBuilder.withPayload(replyObject);
		Map<String, ?> headers = this.headerMapper.toHeaders(amqpReplyMessage.getMessageProperties());
		builder.copyHeadersIfAbsent(headers);
		return builder.build();
	}

}
