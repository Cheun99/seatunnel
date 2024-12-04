/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.starter.seatunnel.command;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.PrettyPrint;
import com.hazelcast.internal.json.WriterConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;
import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/** Use to validate the configuration of the SeaTunnel API. */
@Slf4j
public class SeaTunnelConfValidateCommand implements Command<ClientCommandArgs> {

    private final ClientCommandArgs clientCommandArgs;

    public SeaTunnelConfValidateCommand(ClientCommandArgs clientCommandArgs) {
        this.clientCommandArgs = clientCommandArgs;
    }

    @Override
    public void execute() throws ConfigCheckException {

        Path configPath = clientCommandArgs.getConfigFilePath(clientCommandArgs);
        // TODO: validate config using new api
        FileUtils.checkConfigExist(configPath);
        Config seaTunnelJobConfig = ConfigBuilder.of(Paths.get(configPath.toString()), null);
        ReadonlyConfig env = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
        String jobMode = env.toConfig().getString("job.mode");

        if (!Arrays.asList(JobMode.values()).contains(JobMode.valueOf(jobMode))) {
            throw new JobDefineCheckException("The job.mode option is not configured, please configure it.");
        }
        SeaTunnelConfig seaTunnelConfig = clientCommandArgs.getSeaTunnelConfig();
        ClientConfig clientConfig = clientCommandArgs.getClientConfig();
        String clusterName = clientCommandArgs.getClusterName();
        HazelcastInstance instance = null;
        if (clientCommandArgs.getMasterType().equals(MasterType.LOCAL)) {
            clusterName = clientCommandArgs.creatRandomClusterName(StringUtils.isNotEmpty(clusterName) ? clusterName : Constant.DEFAULT_SEATUNNEL_CLUSTER_NAME);
            instance = createServerInLocal(clusterName, seaTunnelConfig);
            int port = instance.getCluster().getLocalMember().getSocketAddress().getPort();
            clientConfig.getNetworkConfig().setAddresses(Collections.singletonList("localhost:" + port));
        }
        if (StringUtils.isNotEmpty(clusterName)) {
            seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
            clientConfig.setClusterName(clusterName);
        }

        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        ClientJobExecutionEnvironment clientJobExecutionEnvironment = clientCommandArgs.getClientJobExecutionEnvironment(engineClient, seaTunnelConfig);

        LogicalDag logicalDag = clientJobExecutionEnvironment.getLogicalDag();
        JsonObject logicalDagAsJson = logicalDag.getLogicalDagAsJson();
        JsonObject jobInfoJson = new JsonObject();
        jobInfoJson.add(RestConstant.JOB_DAG, logicalDagAsJson);
        WriterConfig config = PrettyPrint.indentWithSpaces(2);
        String dagString = jobInfoJson.toString(config);

        log.info("Parsed job Dag: \n{}", dagString);

        //  jobInfoJson.set();

        Map<String, Map<String, Object>> pluginMaps = new HashMap<>();
        Map<Long, LogicalVertex> logicalVertexMap = logicalDag.getLogicalVertexMap();
        logicalVertexMap.values().stream().map(LogicalVertex::getAction).forEach(
                action -> {
                    if (action instanceof SourceAction) {
                        SourceAction sourceAction = (SourceAction) action;
                        SeaTunnelSource source = sourceAction.getSource();
                        String pluginName = source.getPluginName();
                        List<CatalogTable> sourceCatalogTables = source.getProducedCatalogTables();
                        for (CatalogTable producedCatalogTable : sourceCatalogTables) {
                            buildPluginSchema(pluginMaps, PluginType.SOURCE,pluginName, producedCatalogTable);
                        }
                    }
                    if (action instanceof TransformAction) {
                        TransformAction transformAction = (TransformAction) action;
                        SeaTunnelTransform<?> transform = (SeaTunnelTransform<?>) transformAction.getTransform();
                        String pluginName = transform.getPluginName();
                        CatalogTable transformCatalogTable = transform.getProducedCatalogTable();
                        buildPluginSchema(pluginMaps, PluginType.TRANSFORM,pluginName, transformCatalogTable);
                    }
                }
        );

        System.out.println("pluginMaps = " + pluginMaps);

        String s = JsonUtils.toJsonString(pluginMaps);
        System.out.println("s = " + s);
        ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();
        DEFAULT_OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        try {
            String s1 = DEFAULT_OBJECT_MAPPER.writeValueAsString(pluginMaps);
            System.out.println("s1 = " + s1);
        } catch (JsonProcessingException e) {

        }
        if (engineClient != null) {
            engineClient.close();
            log.info("Closed SeaTunnel client......");
        }
        if (instance != null) {
            instance.shutdown();
            log.info("Closed HazelcastInstance ......");
        }
    }

    private void buildPluginSchema(Map<String, Map<String, Object>> pluginMaps, PluginType plugin,String pluginName, CatalogTable producedCatalogTable) {
        String pluginType = plugin.getType();
        TableSchema tableSchema = producedCatalogTable.getTableSchema();
        List<Column> columns = tableSchema.getColumns();
        Map<String, String> schema = columns.stream().collect(Collectors.toMap(Column::getName, column -> {
            SeaTunnelDataType<?> dataType = column.getDataType();
            return dataType.getTypeClass().getSimpleName();
        }));

        Map<String, Object> map = pluginMaps.get(pluginType);
        Map<String, Object> pluginMap = new HashMap<>();
        pluginMap.put(pluginName, schema);
        if (map == null) {
            pluginMaps.put(pluginType, pluginMap);
        } else {
            map.putAll(pluginMap);
        }
    }


    private HazelcastInstance createServerInLocal(String clusterName, SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        // local mode only support MASTER_AND_WORKER role
        seaTunnelConfig.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.MASTER_AND_WORKER);
        // set local mode
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);
        seaTunnelConfig.getHazelcastConfig().getNetworkConfig().setPortAutoIncrement(true);
        return HazelcastInstanceFactory.newHazelcastInstance(seaTunnelConfig.getHazelcastConfig(), Thread.currentThread().getName(), new SeaTunnelNodeContext(seaTunnelConfig));
    }
}
