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

package org.apache.flink.mesos.dispatcher.store;

import com.google.common.collect.ImmutableList;
import org.apache.mesos.Protos;
import scala.Option;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A standalone Mesos session store.
 */
public class StandaloneMesosSessionStore implements MesosSessionStore {

	private Option<Protos.FrameworkID> frameworkID = Option.empty();

	private int taskCount = 0;

	private Map<Protos.TaskID, Session> storedTasks = new LinkedHashMap<>();

	public StandaloneMesosSessionStore() {
	}

	@Override
	public void start() throws PersistenceException {

	}

	@Override
	public void stop() throws PersistenceException {

	}

	@Override
	public Option<Protos.FrameworkID> getFrameworkID() throws PersistenceException {
		return frameworkID;
	}

	@Override
	public void setFrameworkID(Option<Protos.FrameworkID> frameworkID) throws PersistenceException {
		this.frameworkID = frameworkID;
	}

	@Override
	public List<Session> recoverTasks() throws PersistenceException {
		return ImmutableList.copyOf(storedTasks.values());
	}

	@Override
	public Protos.TaskID newTaskID() throws PersistenceException {
		Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue(TASKID_FORMAT.format(++taskCount)).build();
		return taskID;
	}

	@Override
	public void putTask(Session... tasks) throws PersistenceException {
		for (Session task : tasks) {
			storedTasks.put(task.taskID(), task);
		}
	}

	@Override
	public void removeTask(Protos.TaskID... taskIDs) throws PersistenceException {
		for (Protos.TaskID taskID : taskIDs) {
			storedTasks.remove(taskID);
		}
	}

	@Override
	public void cleanup() throws PersistenceException {
	}
}
