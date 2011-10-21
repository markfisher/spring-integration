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
import java.util.Collections;
import java.util.List;

import org.springframework.integration.Message;
import org.springframework.integration.MessageChannel;
import org.springframework.util.StringUtils;

/**
 * A Message Router that resolves the {@link MessageChannel} based on the
 * {@link Message Message's} payload type.
 * 
 * Selects the most appropriate channel mapping keys which are matched against the
 * fully qualified class names encountered while traversing the payload type hierarchy.
 * To resolve ties and conflicts (e.g., Serializable and String) it will match:
 * 1. Type name to channel mapping key, else...
 * 2. Name of the subclass of the type to channel identifier else...
 * 3. Name of the Interface of the type to channel identifier while also
 *    preferring direct interfaces over indirect subclasses
 * 
 * @author Mark Fisher
 * @author Oleg Zhurakousky
 */
public class PayloadTypeRouter extends AbstractMappingMessageRouter {

	private static final String ARRAY_SUFFIX = "[]";


	@Override
	protected int getMaxDestinations() {
		return 1;
	}

	@Override
	protected boolean shouldFallbackToDirectChannelLookup() {
		return false;
	}

	@Override
	protected List<Object> getChannelKeys(Message<?> message) {
		List<Object> allCandidates = new ArrayList<Object>();
		List<List<String>> classCandidates = new ArrayList<List<String>>();
		List<List<String>> interfaceCandidates = new ArrayList<List<String>>();
		Class<?> payloadType = message.getPayload().getClass();
		boolean isArray = payloadType.isArray();
		if (isArray) {
			payloadType = payloadType.getComponentType();
		}
		this.addCandidates(payloadType, isArray, classCandidates, interfaceCandidates);
		int max = Math.max(classCandidates.size(), interfaceCandidates.size());
		for (int i = 0; i < max; i++) {
			if (classCandidates.size() > i) {
				List<String> l = classCandidates.get(i);
				for (String s : l) {
					if (StringUtils.hasText(s)) {
						allCandidates.add(s);
					}
				}
			}
			if (interfaceCandidates.size() > i) {
				List<String> l = interfaceCandidates.get(i);
				for (String s: l) {
					if (StringUtils.hasText(s)) {
						allCandidates.add(s);
					}
				}
			}
		}
		return allCandidates;
	}

	private void addCandidates(Class<?> type, boolean isArray, List<List<String>> classCandidates, List<List<String>> interfaceCandidates) {
		String typeName = type.getName();
		// first level class
		this.addCandidate(typeName, isArray, classCandidates);
		Class<?>[] interfaces = type.getInterfaces();
		List<String> interfaceTypeNames = new ArrayList<String>();
		for (Class<?> i : interfaces) {
			interfaceTypeNames.add(i.getName());
		}
		// first level interfaces
		this.addCandidate(StringUtils.collectionToCommaDelimitedString(interfaceTypeNames), isArray, interfaceCandidates);
		Class<?> superType = type.getSuperclass();
		while (superType != null) {
			// next level class
			this.addCandidate(superType.getName(), isArray, classCandidates);
			List<String> superInterfaceTypeNames = new ArrayList<String>();
			for (Class<?> i : interfaces) {
				Class<?>[] superInterfaces = i.getInterfaces();
				for (Class<?> superInterface : superInterfaces) {
					superInterfaceTypeNames.add(superInterface.getName());
				}
			}
			Class<?>[] superTypeInterfaces = superType.getInterfaces();
			for (Class<?> superTypeInterface : superTypeInterfaces) {
				superInterfaceTypeNames.add(superTypeInterface.getName());
			}
			// next level interface
			this.addCandidate(StringUtils.collectionToCommaDelimitedString(superInterfaceTypeNames), isArray, interfaceCandidates);
			superType = superType.getSuperclass();
		}
	}
	
	private void addCandidate(String typeName, boolean isArray, List<List<String>> candidates) {
		if (!isArray) {
			if (!this.containsCandidate(typeName, candidates)) {
				candidates.add(Collections.singletonList(typeName));
			}
		}
		else {
			String[] array = StringUtils.commaDelimitedListToStringArray(typeName);
			if (array.length > 1) {
				List<String> keepers = new ArrayList<String>();
				for (int i = 0; i < array.length; i++) {
					String name = array[i] + ARRAY_SUFFIX;
					if (!candidates.contains(name)) {
						keepers.add(name);
					}
				}
				typeName = StringUtils.collectionToCommaDelimitedString(keepers);
				if (!this.containsCandidate(typeName, candidates)) {
					candidates.add(keepers);
				}
			}
			else {
				if (!this.containsCandidate(typeName, candidates)) {
					candidates.add(Collections.singletonList(typeName + ARRAY_SUFFIX));
				}
			}
		}
	}

	private boolean containsCandidate(String candidate, List<List<String>> existingCandidates) {
		for (List<String> innerList : existingCandidates) {
			for (String existingCandidate : innerList) {
				String[] array = StringUtils.commaDelimitedListToStringArray(existingCandidate);
				for (String s : array) {
					if (s.equals(candidate)) {
						return true;
					}
				}
			}
		}
		return false;
	}

}
