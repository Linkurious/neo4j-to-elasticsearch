/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.es.search;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.graphaware.common.log.LoggerFactory;
import com.graphaware.module.es.ElasticSearchConfiguration;
import com.graphaware.module.es.ElasticSearchModule;
import com.graphaware.module.es.mapping.Mapping;
import com.graphaware.module.uuid.UuidModule;
import com.graphaware.module.uuid.read.DefaultUuidReader;
import com.graphaware.module.uuid.read.UuidReader;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.graphaware.runtime.RuntimeRegistry.getStartedRuntime;
import static org.springframework.util.Assert.notNull;

public class Searcher {
    private static final Log LOG = LoggerFactory.getLogger(Searcher.class);

    private final GraphDatabaseService database;
    private final JestClient client;

    private final String keyProperty;
    private final Mapping mapping;
    private final UuidReader uuidReader;

    public Searcher(GraphDatabaseService database) {
        ElasticSearchConfiguration configuration = (ElasticSearchConfiguration) getStartedRuntime(database).getModule(ElasticSearchModule.class).getConfiguration();

        this.keyProperty = configuration.getKeyProperty();
        this.database = database;
        this.uuidReader = createUuidReader(database);
        this.client = createClient(configuration.getUri(), configuration.getPort(), configuration.getAuthUser(), configuration.getAuthPassword());
        this.mapping = configuration.getMapping();
    }

    private UuidReader createUuidReader(GraphDatabaseService database) {
        return new DefaultUuidReader(
                getStartedRuntime(database).getModule(UuidModule.class).getConfiguration(),
                database
        );
    }

    private Function<SearchMatch, Relationship> getRelationshipResolver() {
        return match -> {
            Relationship rel;
            try {
                rel = database.getRelationshipById(uuidReader.getRelationshipIdByUuid(match.uuid));
            } catch(NotFoundException e) {
                rel = null;
            }
            if (rel == null) {
                LOG.warn("Could not find relationship with uuid (" + keyProperty + "): " + match.uuid);
            }
            return rel;
        };
    }

    private Function<SearchMatch, Node> getNodeResolver() {
        return match -> {
            Node node = null;
            try {
                node = database.getNodeById(uuidReader.getNodeIdByUuid(match.uuid));
            } catch (NotFoundException e){
                node = null;
            }
            if (node == null) {
                LOG.warn("Could not find node with uuid (" + keyProperty + "): " + match.uuid);
            }
            return node;
        };
    }

    private <T extends PropertyContainer> List<SearchMatch<T>> resolveMatchItems(List<SearchMatch<T>> searchMatches, Function<SearchMatch, T> resolver) {
        List<SearchMatch<T>> resolvedResults = new ArrayList<>();

        try (Transaction tx = database.beginTx()) {
            searchMatches.stream().forEach(match -> {
                T item = resolver.apply(match);
                if (item != null) {
                    match.setItem(item);
                    resolvedResults.add(match);
                }
            });
            tx.success();
        }
        return resolvedResults;
    }

    private <T extends PropertyContainer> List<SearchMatch<T>> buildSearchMatches(SearchResult searchResult) {
        List<SearchMatch<T>> matches = new ArrayList<>();
        Set<Map.Entry<String, JsonElement>> entrySet = searchResult.getJsonObject().entrySet();
        entrySet.stream()
                .filter((item) -> (item.getKey().equalsIgnoreCase("hits")))
                .map((item) -> (JsonObject) item.getValue())
                .filter((hits) -> (hits != null))
                .map((hits) -> hits.getAsJsonArray("hits"))
                .filter((hitsArray) -> (hitsArray != null))
                .forEach((hitsArray) -> {
                    for (JsonElement element : hitsArray) {
                        JsonObject obj = (JsonObject) element;
                        Double score = obj.get("_score").getAsDouble();
                        String keyValue = obj.get("_id") != null ? obj.get("_id").getAsString() : null;
                        if (keyValue == null) {
                            LOG.warn("No key found in search result: " + obj.getAsString());
                        } else {
                            matches.add(new SearchMatch<>(keyValue, score));
                        }
                    }
                });
        return matches;
    }

    public static JestClient createClient(String uri, String port, String authUser, String authPassword) {
        notNull(uri);
        notNull(port);

        LOG.info("Creating Jest Client...");

        JestClientFactory factory = new JestClientFactory();
        String esHost = String.format("http://%s:%s", uri, port);
        HttpClientConfig.Builder clientConfigBuilder = new HttpClientConfig.Builder(esHost).multiThreaded(true);

        if (authUser != null && authPassword != null) {
            BasicCredentialsProvider customCredentialsProvider = new BasicCredentialsProvider();

            customCredentialsProvider.setCredentials(
                    new AuthScope(uri, Integer.parseInt(port)),
                    new UsernamePasswordCredentials(authUser, authPassword)
            );

            LOG.info("Enabling Auth for ElasticSearch: " + authUser);
            clientConfigBuilder.credentialsProvider(customCredentialsProvider);
        }

        factory.setHttpClientConfig(clientConfigBuilder.build());

        LOG.info("Created Jest Client.");

        return factory.getObject();
    }

    /**
     *
     * @param query The query to send to the index
     * @param clazz {@link Node} or {@link Relationship}, to decide which index to send the query to.
     * @param <T> {@link Node} or {@link Relationship}
     * @return the query response
     */
    private <T extends PropertyContainer> SearchResult doQuery(String query, Class<T> clazz) {
        Search search = new Search.Builder(query).addIndex(mapping.getIndexFor(clazz)).build();

        SearchResult result;
        try {
            result = client.execute(search);
        } catch (IOException ex) {
            throw new RuntimeException("Error while performing query on ElasticSearch", ex);
        }
        return result;
    }

    /**
     * Search for nodes or relationships
     *
     * @param query An ElasticSearch query in JSON format (serialized as a string)
     * @param clazz {@link Node} or {@link Relationship}
     * @param <T> {@link Node} or {@link Relationship}
     * @return a list of matches (with node or a relationship)
     */
    public <T extends PropertyContainer> List<SearchMatch<T>> search(String query, Class<T> clazz) {
        SearchResult result = doQuery(query, clazz);

        List<SearchMatch<T>> matches = buildSearchMatches(result);
        @SuppressWarnings("unchecked")
        Function<SearchMatch, T> resolver = (Function<SearchMatch, T>) (
                clazz.equals(Node.class) ? getNodeResolver() : getRelationshipResolver()
        );
        return resolveMatchItems(matches, resolver);
    }

    /**
     *
     * @param query The search query
     * @param clazz The index key ({@link Node} or {@link Relationship})
     * @param <T> {@link Node} or {@link Relationship}
     * @return a JSON string
     */
    public <T extends PropertyContainer> String rawSearch(String query, Class<T> clazz) {
        SearchResult r = doQuery(query, clazz);
        return r.getJsonString();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (client == null) { return; }
        client.shutdownClient();
    }
}
