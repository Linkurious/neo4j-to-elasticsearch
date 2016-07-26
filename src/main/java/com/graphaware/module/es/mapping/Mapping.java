/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.es.mapping;

import com.graphaware.writer.thirdparty.WriteOperation;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.neo4j.graphdb.PropertyContainer;

import java.util.List;
import java.util.Map;

public interface Mapping {

    /**
     * Used to retrieve the configuration of the module
     *
     * @param config The module configuration map
     */
    void configure(Map<String, String> config);

    /**
     *
     * @param operation An operation on a node or relationship in Neo4j
     * @return a list of actions to perform in ElasticSearch reflect the new state of the affected Neo4j data.
     */
    List<BulkableAction<? extends JestResult>> getActions(WriteOperation operation);

    /**
     * Create the required indexes and mappings in ElasticSearch.
     * Is responsible for checking is the required indexes already exist.
     * Called after configure.
     *
     * @param client An ElasticSearch client
     * @throws Exception
     */
    void createIndexAndMapping(JestClient client) throws Exception;

    /**
     * Given the element class, return the name of the ElasticSearch index in which the element is stored.
     *
     * @param searchedType the class {@link org.neo4j.graphdb.Node} or {@link org.neo4j.graphdb.Relationship}
     * @param <T> {@link org.neo4j.graphdb.Node} or {@link org.neo4j.graphdb.Relationship}
     * @return the name of an ElasticSearch index
     */
    <T extends PropertyContainer> String getIndexFor(Class<T> searchedType);

    /**
     * @return The name of the Neo4j node/relationship property used as key (_id) in ElasticSearch (usually "uuid").
     */
    String getKeyProperty();

}
