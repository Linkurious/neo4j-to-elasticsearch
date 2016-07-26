package com.graphaware.module.es.mapping;

import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.common.representation.RelationshipRepresentation;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.indices.mapping.PutMapping;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import java.util.*;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class AltMapping extends BaseMapping {

    public AltMapping() { }

    @Override
    protected List<BulkableAction<? extends JestResult>> deleteNode(NodeRepresentation node) {
        String id = getKey(node);
        return singletonList(
                new Delete.Builder(id).index(getIndexFor(Node.class)).build()
        );
    }

    @Override
    protected List<BulkableAction<? extends JestResult>> deleteRelationship(RelationshipRepresentation r) {
        return singletonList(
                new Delete.Builder(getKey(r)).index(getIndexFor(Relationship.class)).build()
        );
    }

    @Override
    protected List<BulkableAction<? extends JestResult>> updateNode(NodeRepresentation before, NodeRepresentation after) {
        return singletonList(createOrUpdateNode(after));
    }

    @Override
    protected List<BulkableAction<? extends JestResult>> updateRelationship(RelationshipRepresentation before, RelationshipRepresentation after) {
        return singletonList(createOrUpdateRelationship(after));
    }

    @Override
    protected List<BulkableAction<? extends JestResult>> createNode(NodeRepresentation node) {
        return singletonList(createOrUpdateNode(node));
    }

    @Override
    protected List<BulkableAction<? extends JestResult>> createRelationship(RelationshipRepresentation relationship) {
        return singletonList(createOrUpdateRelationship(relationship));
    }

    private BulkableAction<? extends JestResult> createOrUpdateNode(NodeRepresentation node) {
        return new Index.Builder(map(node)).index(getIndexFor(Node.class)).id(getKey(node)).build();
    }

    private BulkableAction<? extends JestResult> createOrUpdateRelationship(RelationshipRepresentation r) {
        return new Index.Builder(map(r)).index(getIndexFor(Relationship.class)).id(getKey(r)).build();
    }

    @Override
    protected Map<String, String> map(NodeRepresentation node) {
        Map<String, String> m = super.map(node);
        // todo: back merge changes from 23-dont-index-uuid before uncommenting
        // m.put("__type", Arrays.asList(node.getLabels()));
        return m;
    }

    @Override
    protected Map<String, String> map(RelationshipRepresentation relationship) {
        Map<String, String> m = super.map(relationship);
        m.put("__type", relationship.getType());
        return m;
    }

    @Override
    protected void createIndexAndMapping(JestClient client, String index) throws Exception {
        super.createIndexAndMapping(client, index);

        Map<String, Object> rawField = new HashMap<>();
        rawField.put("type", "string");
        rawField.put("index", "not_analyzed");
        rawField.put("stored", false);
        rawField.put("include_in_all", false);

        Map<String, Object> typeMapping = new HashMap<>();
        typeMapping.put("type", "string");
        typeMapping.put("fields", singletonMap("raw", rawField));

        Map<String, Object> mapping = singletonMap("_default_", singletonMap("properties", singletonMap("__type", typeMapping)));

        client.execute(new PutMapping.Builder(index, "_default_", mapping).build());
    }

    @Override
    public <T extends PropertyContainer> String getIndexFor(Class<T> searchedType) {
        return getIndexPrefix() + (searchedType.equals(Node.class) ? "-nodeALT" : "-relationshipALT");
    }
}
