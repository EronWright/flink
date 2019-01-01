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

package org.apache.flink.table.client.gateway.utils;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalTableUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

/**
 * Utilities for executors.
 */
public class ExecutorUtil {

	public static List<String> listTables(TableEnvironment tableEnv) {
		List<String> output = new ArrayList<>();
		Stack<String> stack = new Stack<>();

		// process the root tables
		output.addAll(Arrays.asList(tableEnv.listTables()));

		// process the catalogs
		for (String catalogName : tableEnv.listExternalCatalogs()) {
			ExternalCatalog catalog = tableEnv.getRegisteredExternalCatalog(catalogName);
			stack.push(catalogName);
			listTablesRecursive(stack, catalog, output);
			stack.pop();
		}

		return output;
	}

	private static void listTablesRecursive(Stack<String> stack, ExternalCatalog catalog, List<String> output) {
		// process the tables in this catalog
		for (String table : catalog.listTables()) {
			stack.push(table);
			output.add(ExternalTableUtil.makeQualifiedTableName(stack));
			stack.pop();
		}

		// process the sub-catalogs
		for (String dbName : catalog.listSubCatalogs()) {
			stack.push(dbName);
			listTablesRecursive(stack, catalog.getSubCatalog(dbName), output);
			stack.pop();
		}
	}
}
