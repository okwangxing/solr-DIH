package org.apache.solr.handler.dataimport.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.handler.dataimport.DataImportHandlerException;
import org.apache.solr.handler.dataimport.DataImporter;
import org.apache.solr.handler.dataimport.SqlEntityProcessor;
import org.w3c.dom.Element;

import com.google.common.collect.Maps;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * <p>
 * Mapping for data-config.xml
 * </p>
 * <p/>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache
 * .org/solr/DataImportHandler</a> for more details.
 * </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 * 
 * @since solr 1.3
 */
public class DIHConfiguration {
	// TODO - remove from here and add it to entity
	private final String deleteQuery;

	private final List<Entity> entities;
	private final String onImportStart;
	private final String onImportEnd;
	private final List<Map<String, String>> functions;
	private final Script script;
	private final Map<String, Map<String, String>> dataSources;
	private final PropertyWriter propertyWriter;

	public DIHConfiguration(Element element, DataImporter di, List<Map<String, String>> functions, Script script, Map<String, Map<String, String>> dataSources, PropertyWriter pw) {
		this.deleteQuery = ConfigParseUtil.getStringAttribute(element, "deleteQuery", null);
		this.onImportStart = ConfigParseUtil.getStringAttribute(element, "onImportStart", null);
		this.onImportEnd = ConfigParseUtil.getStringAttribute(element, "onImportEnd", null);
		List<Entity> modEntities = new ArrayList<Entity>();
		List<Element> l = ConfigParseUtil.getChildNodes(element, "entity");
		boolean docRootFound = false;
		 Map<String, Map<String, String>> dataSources2 = Maps.newHashMap();
		for (Element e : l) {
			HashMap<String, String> attrs = ConfigParseUtil.getAllAttributes(e);

			// 分库分表的时候,entity上必须设置datasource_%d
			if (attrs.containsKey("shard") && attrs.get("shard").equals("true")) {
				String datasource = attrs.get("datasource");
				Map<String, String> ds = dataSources.get(datasource);
				int shardStep = Integer.valueOf(ds.get("shardStep"));
				int shardBegin = Integer.valueOf(ds.get("shardBegin"));
				int shardEnd = Integer.valueOf(ds.get("shardEnd"));

				for (int i = shardBegin; i <= shardEnd; i += shardStep) {
					Map<String, String> pp = new HashMap<String, String>();
					String connectStr = String.format(ds.get("url"), i);
					for (Map.Entry<String, String> dsAttr : ds.entrySet()) {
						if (dsAttr.getKey().equals("url")) {
							pp.put(dsAttr.getKey(), connectStr);
						} else if (dsAttr.getKey().equals("shardStep")) {
						} else if (dsAttr.getKey().equals("shardBegin")) {
						} else if (dsAttr.getKey().equals("shardEnd")) {
						} else if (dsAttr.getKey().equals("shard")) {
						} else {
							pp.put(dsAttr.getKey(), dsAttr.getValue());
						}
					}
					String dsName = String.format(datasource, i);
					dataSources2.put(dsName, pp);

					HashMap<String, String> tableAttrs = ConfigParseUtil.getAllAttributes(e);
					if (tableAttrs.containsKey("shard") && tableAttrs.get("shard").equals("true")) {
						int tableShardStep = Integer.valueOf(tableAttrs.get("shardStep"));
						int tableShardBegin = Integer.valueOf(tableAttrs.get("shardBegin"));
						int tableShardEnd = Integer.valueOf(tableAttrs.get("shardEnd"));

						for (int j = tableShardBegin; j <= tableShardEnd; j += tableShardStep) {
							HashMap<String, String> newTableAttrs = Maps.newHashMap();
							
							String sqlstr = null;
							String deltaSqlstr = null;
							String deltaImportSqlstr = null;
							String delPkSqlstr = null;
							
							String formatStr = tableAttrs.get(SqlEntityProcessor.QUERY);
							if(!StringUtils.isEmpty(formatStr)){
								sqlstr = String.format(formatStr, j); 
							}
							formatStr = tableAttrs.get(SqlEntityProcessor.DELTA_QUERY);
							if(!StringUtils.isEmpty(formatStr)){
								deltaSqlstr = String.format(formatStr, j); 
							}
							formatStr = tableAttrs.get(SqlEntityProcessor.DELTA_IMPORT_QUERY);
							if(!StringUtils.isEmpty(formatStr)){
								deltaImportSqlstr = String.format(formatStr, j); 
							}
							formatStr = tableAttrs.get(SqlEntityProcessor.DEL_PK_QUERY);
							if(!StringUtils.isEmpty(formatStr)){
								delPkSqlstr = String.format(formatStr, j); 
							}
							
							for (Map.Entry<String, String> tableAttr : tableAttrs.entrySet()) {
								if (tableAttr.getKey().equals(SqlEntityProcessor.QUERY) && sqlstr != null) {
									newTableAttrs.put(tableAttr.getKey(), sqlstr);
								} else if (tableAttr.getKey().equals(SqlEntityProcessor.DELTA_QUERY) && deltaSqlstr != null) {
									newTableAttrs.put(tableAttr.getKey(), deltaSqlstr);
								} else if (tableAttr.getKey().equals(SqlEntityProcessor.DELTA_IMPORT_QUERY) && deltaImportSqlstr != null) {
									newTableAttrs.put(tableAttr.getKey(), deltaImportSqlstr);
								} else if (tableAttr.getKey().equals(SqlEntityProcessor.DEL_PK_QUERY) && delPkSqlstr != null) {
									newTableAttrs.put(tableAttr.getKey(), delPkSqlstr);
								} else if (tableAttr.getKey().equals("shardStep")) {
								} else if (tableAttr.getKey().equals("shardBegin")) {
								} else if (tableAttr.getKey().equals("shardEnd")) {
								} else if (tableAttr.getKey().equals("shard")) {
								} else {
									newTableAttrs.put(tableAttr.getKey(), tableAttr.getValue());
								}
							}

							Entity entity = new Entity(docRootFound, e, di, null, true, dsName, newTableAttrs);
							Map<String, EntityField> fields = ConfigParseUtil.gatherAllFields(di, entity);
							ConfigParseUtil.verifyWithSchema(di, fields);
							modEntities.add(entity);
						}

					} else {
						throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "SolrDataImport: parameter 'shard' is required");
					}
				}

			} else {
				Entity entity = new Entity(docRootFound, e, di, null);
				Map<String, EntityField> fields = ConfigParseUtil.gatherAllFields(di, entity);
				ConfigParseUtil.verifyWithSchema(di, fields);
				modEntities.add(entity);
				dataSources2 = dataSources;
			}
		}
		this.entities = Collections.unmodifiableList(modEntities);
		if (functions == null) {
			functions = Collections.emptyList();
		}
		List<Map<String, String>> modFunc = new ArrayList<Map<String, String>>(functions.size());
		for (Map<String, String> f : functions) {
			modFunc.add(Collections.unmodifiableMap(f));
		}
		this.functions = Collections.unmodifiableList(modFunc);
		this.script = script;
		this.dataSources = Collections.unmodifiableMap(dataSources2);
		this.propertyWriter = pw;
	}

	public String getDeleteQuery() {
		return deleteQuery;
	}

	public List<Entity> getEntities() {
		return entities;
	}

	public String getOnImportStart() {
		return onImportStart;
	}

	public String getOnImportEnd() {
		return onImportEnd;
	}

	public List<Map<String, String>> getFunctions() {
		return functions;
	}

	public Map<String, Map<String, String>> getDataSources() {
		return dataSources;
	}

	public Script getScript() {
		return script;
	}

	public PropertyWriter getPropertyWriter() {
		return propertyWriter;
	}
}