/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Heap-backed partitioned {@link org.apache.flink.api.common.state.ReducingState} that is
 * snapshotted into files.
 * 
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class FsReducingState<K, N, V>
	extends AbstractFsState<K, N, V, ReducingState<V>, ReducingStateDescriptor<V>>
	implements ReducingState<V> {

	private final ReduceFunction<V> reduceFunction;

	/**
	 * Creates a new and empty partitioned state.
	 *
	 * @param backend The file system state backend backing snapshots of this state
	 * @param keySerializer The serializer for the key.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 */
	public FsReducingState(FsStateBackend backend,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		ReducingStateDescriptor<V> stateDesc) {
		super(backend, keySerializer, namespaceSerializer, stateDesc.getSerializer(), stateDesc);
		this.reduceFunction = stateDesc.getReduceFunction();
	}

	/**
	 * Creates a new key/value state with the given state contents.
	 * This method is used to re-create key/value state with existing data, for example from
	 * a snapshot.
	 *
	 * @param backend The file system state backend backing snapshots of this state
	 * @param keySerializer The serializer for the key.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
*                           and can create a default state value.
	 * @param state The map of key/value pairs to initialize the state with.
	 */
	public FsReducingState(FsStateBackend backend,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		ReducingStateDescriptor<V> stateDesc,
		HashMap<N, Map<K, V>> state) {
		super(backend, keySerializer, namespaceSerializer, stateDesc.getSerializer(), stateDesc, state);
		this.reduceFunction = stateDesc.getReduceFunction();
	}


	@Override
	public V get() {
		if (currentNSState == null) {
			Preconditions.checkState(currentNamespace != null, "No namespace set");
			currentNSState = state.get(currentNamespace);
		}
		if (currentNSState != null) {
			Preconditions.checkState(currentKey != null, "No key set");
			return currentNSState.get(currentKey);
		}
		return null;
	}

	@Override
	public void add(V value) throws IOException {
		Preconditions.checkState(currentKey != null, "No key set");

		if (currentNSState == null) {
			Preconditions.checkState(currentNamespace != null, "No namespace set");
			currentNSState = createNewNamespaceMap();
			state.put(currentNamespace, currentNSState);
		}
//		currentKeyState.merge(currentNamespace, value, new BiFunction<V, V, V>() {
//			@Override
//			public V apply(V v, V v2) {
//				try {
//					return reduceFunction.reduce(v, v2);
//				} catch (Exception e) {
//					return null;
//				}
//			}
//		});
		V currentValue = currentNSState.get(currentKey);
		if (currentValue == null) {
			currentNSState.put(currentKey, value);
		} else {
			try {
				currentNSState.put(currentKey, reduceFunction.reduce(currentValue, value));
			} catch (Exception e) {
				throw new RuntimeException("Could not add value to reducing state.", e);
			}
		}
	}
	@Override
	public KvStateSnapshot<K, N, ReducingState<V>, ReducingStateDescriptor<V>, FsStateBackend> createHeapSnapshot(Path filePath) {
		return new Snapshot<>(getKeySerializer(), getNamespaceSerializer(), stateSerializer, stateDesc, filePath);
	}

	@Override
	public byte[] getSerializedValue(K key, N namespace) throws Exception {
		Preconditions.checkNotNull(key, "Key");
		Preconditions.checkNotNull(namespace, "Namespace");

		Map<K, V> stateByKey = state.get(namespace);
		if (stateByKey != null) {
			return KvStateRequestSerializer.serializeValue(stateByKey.get(key), stateDesc.getSerializer());
		} else {
			return null;
		}
	}

	public static class Snapshot<K, N, V> extends AbstractFsStateSnapshot<K, N, V, ReducingState<V>, ReducingStateDescriptor<V>> {
		private static final long serialVersionUID = 1L;

		public Snapshot(TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> stateSerializer,
			ReducingStateDescriptor<V> stateDescs,
			Path filePath) {
			super(keySerializer, namespaceSerializer, stateSerializer, stateDescs, filePath);
		}

		@Override
		public KvState<K, N, ReducingState<V>, ReducingStateDescriptor<V>, FsStateBackend> createFsState(FsStateBackend backend, HashMap<N, Map<K, V>> stateMap) {
			return new FsReducingState<>(backend, keySerializer, namespaceSerializer, stateDesc, stateMap);
		}
	}
}
