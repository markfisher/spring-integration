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

import org.jivesoftware.smack.packet.Message;
import org.springframework.integration.mapping.HeaderMapper;

/**
 * A convenience interface that extends {@link HeaderMapper}
 * but parameterized with {@link MessageProperties}.
 *
 * @author Mark Fisher
 * @since 2.1
 */
public interface XmppHeaderMapper extends HeaderMapper<Message> {
}