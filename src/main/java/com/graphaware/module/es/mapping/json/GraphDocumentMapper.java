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
package com.graphaware.module.es.mapping.json;

import com.graphaware.common.log.LoggerFactory;
import com.graphaware.common.representation.NodeRepresentation;
import com.graphaware.common.representation.PropertyContainerRepresentation;
import com.graphaware.common.representation.RelationshipRepresentation;
import org.neo4j.logging.Log;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.HashMap;
import java.util.Map;
import org.springframework.expression.ParseException;

public class GraphDocumentMapper {

    private static final Log LOG = LoggerFactory.getLogger(GraphDocumentMapper.class);

    private String condition;

    private String index;

    private String type;

    private Map<String, String> properties;
    
    private SpelExpressionParser expressionParser;
    
    //Some cache to avoid continous parsing
    private Map<String, Expression> expressions;
    private Expression typeExpression;
    private Map<String, Expression> indexsExpression;
    
    
    public String getCondition() {
        return condition;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean supports(PropertyContainerRepresentation element) {
        if (null == condition) {
            return false;
        }

        try {
            Expression expression = getExpressionParser().parseExpression(condition);

            if (element instanceof NodeRepresentation) {
                return (Boolean) expression.getValue(new NodeExpression((NodeRepresentation) element));
            } else if (element instanceof RelationshipRepresentation) {
                return (Boolean) expression.getValue(new RelationshipExpression((RelationshipRepresentation) element));
            }
        } catch (Exception e) {
            LOG.error("Invalid condition expression {}", condition);
        }

        return false;
    }

    public DocumentRepresentation getDocumentRepresentation(NodeRepresentation node, DocumentMappingDefaults defaults) throws DocumentRepresentationException {
        return getDocumentRepresentation(node, defaults, true);
    }

    public DocumentRepresentation getDocumentRepresentation(NodeRepresentation node, DocumentMappingDefaults defaults, boolean buildSource) throws DocumentRepresentationException {
        NodeExpression nodeExpression = new NodeExpression(node);
        Map<String, Object> source = new HashMap<>();
        String i = getIndex(nodeExpression, defaults.getDefaultNodesIndex());
        String id = getKeyProperty(node, defaults.getKeyProperty());

        if (buildSource) {
            if (null != properties) {
                for (String s : properties.keySet()) {
                    Expression exp = getExpression(s);
                    source.put(s, exp.getValue(nodeExpression));
                }
            }

            if (defaults.includeRemainingProperties()) {
                for (String s : node.getProperties().keySet()) {
                    if (!defaults.getBlacklistedNodeProperties().contains(s)) {
                        source.put(s, node.getProperties().get(s));
                    }
                }
            }
        }
        return new DocumentRepresentation(i, getType(nodeExpression), id, source);
    }

    private String getKeyProperty(NodeRepresentation node, String keyProperty) throws DocumentRepresentationException {
        Object keyValue = node.getProperties().get(keyProperty);
        if (keyValue == null)
            throw new DocumentRepresentationException(keyProperty);
        return keyValue.toString();
    }
    
    private String getKeyProperty(RelationshipRepresentation relationship, String keyProperty) throws DocumentRepresentationException {
        Object keyValue = relationship.getProperties().get(keyProperty);
        if (keyValue == null)
            throw new DocumentRepresentationException(keyProperty);
        return keyValue.toString();
    }

    protected String getType(PropertyContainerExpression expression) {
        String t;
        if (getTypeExpression() != null)
            t = getTypeExpression().getValue(expression).toString();        
        else
            t = type;

        if (t == null || t.equals("")) {
            LOG.error("Unable to build type name");
            throw new RuntimeException("Unable to build type name");
        }

        return t;
    }

    protected String getIndex(PropertyContainerExpression expression, String defaultIndex)
    {
        String indexName;
        if (getIndexExpression(defaultIndex) != null)
            indexName = getIndexExpression(defaultIndex).getValue(expression).toString();
        else 
            indexName = index != null ? index : defaultIndex;
                
        if (indexName == null || indexName.equals("")) {
            LOG.error("Unable to build index name");
            throw new RuntimeException("Unable to build index name");
        }

        return indexName;
    }

    public DocumentRepresentation getDocumentRepresentation(RelationshipRepresentation relationship, DocumentMappingDefaults defaults) throws DocumentRepresentationException {
        return getDocumentRepresentation(relationship, defaults, true);
    }

    public DocumentRepresentation getDocumentRepresentation(RelationshipRepresentation relationship, DocumentMappingDefaults defaults, boolean buildSource) throws DocumentRepresentationException {
        RelationshipExpression relationshipExpression = new RelationshipExpression(relationship);
        Map<String, Object> source = new HashMap<>();

        if (buildSource) {
            if (null != properties) {
                for (String s : properties.keySet()) {
                    Expression exp = getExpression(s);
                    source.put(s, exp.getValue(relationshipExpression));
                }
            }

            if (defaults.includeRemainingProperties()) {
                for (String s : relationship.getProperties().keySet()) {
                    if (!defaults.getBlacklistedRelationshipProperties().contains(s)) {
                        source.put(s, relationship.getProperties().get(s));
                    }
                }
            }
        }
        String i = getIndex(relationshipExpression, defaults.getDefaultRelationshipsIndex());
        String id = getKeyProperty(relationship, defaults.getKeyProperty());
        return new DocumentRepresentation(i, getType(relationshipExpression), id, source);

    }

    private SpelExpressionParser getExpressionParser() {
        if (null == expressionParser) {
            expressionParser = new SpelExpressionParser();
        }

        return expressionParser;
    }        
    
    private Expression getExpression(String key) {
        if (null == expressions) {
            expressions = new HashMap<>();
        }
        if (expressions.containsKey(key)) {
            return expressions.get(key);
        } else {
            if (!properties.containsKey(key)) {
                throw new RuntimeException("Properties doesn't contains key: " + key);
            }
            Expression parsedExpression = getExpressionParser().parseExpression(properties.get(key));
            expressions.put(key, parsedExpression);
            return parsedExpression;
        }
    }
    
    private Expression getTypeExpression() throws ParseException {
        if (type != null && type.contains("(") && type.contains(")")) {
            if (typeExpression != null) {
                return typeExpression;
            }
            typeExpression = getExpressionParser().parseExpression(type);
        }
        return typeExpression;
    }
    
    private Expression getIndexExpression(String defaultIndex) throws ParseException {
        String indexName = index != null ? index : defaultIndex;
        if (indexName != null && indexName.contains("(") && indexName.contains(")")) {
            if (null == indexsExpression) {
                indexsExpression = new HashMap<>();
            }
            if (indexsExpression.containsKey(indexName)) {
                return indexsExpression.get(indexName);
            }
            Expression indexExpression;
            indexExpression = getExpressionParser().parseExpression(indexName);
            indexsExpression.put(indexName, indexExpression);
            return indexExpression;
        } else {
            return null;
        }
        
        
    }
}
