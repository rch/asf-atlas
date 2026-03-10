/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graphdb.age;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class AtlasAgeElement implements AtlasElement {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAgeElement.class);

    protected final AtlasAgeGraph graph;

    protected AtlasAgeElement(AtlasAgeGraph graph) {
        this.graph = graph;
    }

    protected abstract long getElementId();
    protected abstract Map<String, Object> getElementProperties();
    protected abstract boolean isVertex();
    protected abstract boolean isElementRemoved();

    @Override
    public Object getId() {
        return getElementId();
    }

    @Override
    public Collection<? extends String> getPropertyKeys() {
        Map<String, Object> props = getElementProperties();
        return props != null ? props.keySet() : Collections.emptySet();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getProperty(String propertyName, Class<T> clazz) {
        Map<String, Object> props = getElementProperties();

        if (props == null) {
            return null;
        }

        Object value = props.get(propertyName);

        if (value == null) {
            return null;
        }

        if (AtlasEdge.class == clazz) {
            return (T) graph.getEdge(value.toString());
        }

        if (AtlasVertex.class == clazz) {
            return (T) graph.getVertex(value.toString());
        }

        return convertValue(value, clazz);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Collection<T> getPropertyValues(String propertyName, Class<T> type) {
        T value = getProperty(propertyName, type);
        if (value == null) {
            return Collections.emptyList();
        }
        return Collections.singleton(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<String> getListProperty(String propertyName) {
        Object value = getElementProperties().get(propertyName);

        if (value == null) {
            return null;
        }

        if (value instanceof List) {
            return (List<String>) value;
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> List<V> getListProperty(String propertyName, Class<V> elementType) {
        List<String> value = getListProperty(propertyName);

        if (value == null || value.isEmpty()) {
            return (List<V>) value;
        }

        if (AtlasEdge.class.isAssignableFrom(elementType)) {
            return (List<V>) Lists.transform(value,
                    (Function<String, AtlasEdge<?, ?>>) input -> graph.getEdge(input));
        }

        if (AtlasVertex.class.isAssignableFrom(elementType)) {
            return (List<V>) Lists.transform(value,
                    (Function<String, AtlasVertex<?, ?>>) input -> graph.getVertex(input));
        }

        return (List<V>) value;
    }

    @Override
    public void setListProperty(String propertyName, List<String> values) {
        setProperty(propertyName, values);
    }

    @Override
    public void setPropertyFromElementsIds(String propertyName, List<AtlasElement> values) {
        List<String> ids = new ArrayList<>(values.size());
        for (AtlasElement element : values) {
            ids.add(element.getId().toString());
        }
        setListProperty(propertyName, ids);
    }

    @Override
    public void setPropertyFromElementId(String propertyName, AtlasElement value) {
        setProperty(propertyName, value.getId().toString());
    }

    @Override
    public void removeProperty(String propertyName) {
        try {
            if (isVertex()) {
                graph.getCypherExecutor().removeVertexProperty(getElementId(), propertyName);
            } else {
                graph.getCypherExecutor().removeEdgeProperty(getElementId(), propertyName);
            }
            getElementProperties().remove(propertyName);
            syncToShadow();
        } catch (Exception e) {
            throw new RuntimeException("Failed to remove property " + propertyName, e);
        }
    }

    @Override
    public void removePropertyValue(String propertyName, Object propertyValue) {
        Object currentValue = getElementProperties().get(propertyName);

        if (currentValue instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) currentValue;
            list.remove(propertyValue);
            setProperty(propertyName, list);
        } else if (Objects.equals(currentValue, propertyValue)) {
            removeProperty(propertyName);
        }
    }

    @Override
    public void removeAllPropertyValue(String propertyName, Object propertyValue) {
        Object currentValue = getElementProperties().get(propertyName);

        if (currentValue instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> list = new ArrayList<>((List<Object>) currentValue);
            list.removeIf(item -> Objects.equals(item, propertyValue));
            setProperty(propertyName, list);
        } else if (Objects.equals(currentValue, propertyValue)) {
            removeProperty(propertyName);
        }
    }

    @Override
    public <T> void setProperty(String propertyName, T value) {
        try {
            if (value == null) {
                Object existing = getElementProperties().get(propertyName);
                if (existing != null) {
                    removeProperty(propertyName);
                }
                return;
            }

            if (isVertex()) {
                graph.getCypherExecutor().setVertexProperty(getElementId(), propertyName, value);
            } else {
                graph.getCypherExecutor().setEdgeProperty(getElementId(), propertyName, value);
            }
            getElementProperties().put(propertyName, value);
            syncToShadow();
        } catch (Exception e) {
            throw new RuntimeException("Failed to set property " + propertyName, e);
        }
    }

    @Override
    public JSONObject toJson(Set<String> propertyKeys) throws JSONException {
        JSONObject json = new JSONObject();
        json.put("_id", getElementId());

        Map<String, Object> props = getElementProperties();
        Set<String> keys = propertyKeys != null ? propertyKeys : (Set<String>) props.keySet();

        for (String key : keys) {
            Object value = props.get(key);
            if (value != null) {
                json.put(key, value);
            }
        }

        return json;
    }

    @Override
    public boolean exists() {
        return !isElementRemoved();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void setJsonProperty(String propertyName, T value) {
        if (value != null) {
            setProperty(propertyName, value.toString());
        } else {
            removeProperty(propertyName);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getJsonProperty(String propertyName) {
        return (T) getProperty(propertyName, String.class);
    }

    @Override
    public String getIdForDisplay() {
        return String.valueOf(getElementId());
    }

    @Override
    public boolean isIdAssigned() {
        return getElementId() > 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getWrappedElement() {
        return null; // No underlying TinkerPop element
    }

    protected void syncToShadow() {
        try {
            Map<String, Object> props = getElementProperties();
            if (isVertex()) {
                graph.getCypherExecutor().syncVertexToShadow(getElementId(), props);
            } else {
                graph.getCypherExecutor().syncEdgeToShadow(getElementId(), props);
            }
        } catch (Exception e) {
            LOG.warn("Failed to sync element {} to shadow table", getElementId(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T convertValue(Object value, Class<T> clazz) {
        if (clazz.isInstance(value)) {
            return (T) value;
        }

        if (clazz == String.class) {
            return (T) value.toString();
        }

        if (clazz == Long.class || clazz == long.class) {
            if (value instanceof Number) return (T) Long.valueOf(((Number) value).longValue());
            return (T) Long.valueOf(value.toString());
        }

        if (clazz == Integer.class || clazz == int.class) {
            if (value instanceof Number) return (T) Integer.valueOf(((Number) value).intValue());
            return (T) Integer.valueOf(value.toString());
        }

        if (clazz == Double.class || clazz == double.class) {
            if (value instanceof Number) return (T) Double.valueOf(((Number) value).doubleValue());
            return (T) Double.valueOf(value.toString());
        }

        if (clazz == Float.class || clazz == float.class) {
            if (value instanceof Number) return (T) Float.valueOf(((Number) value).floatValue());
            return (T) Float.valueOf(value.toString());
        }

        if (clazz == Boolean.class || clazz == boolean.class) {
            if (value instanceof Boolean) return (T) value;
            return (T) Boolean.valueOf(value.toString());
        }

        if (clazz == Short.class || clazz == short.class) {
            if (value instanceof Number) return (T) Short.valueOf(((Number) value).shortValue());
            return (T) Short.valueOf(value.toString());
        }

        if (clazz == Byte.class || clazz == byte.class) {
            if (value instanceof Number) return (T) Byte.valueOf(((Number) value).byteValue());
            return (T) Byte.valueOf(value.toString());
        }

        return (T) value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof AtlasAgeElement)) return false;
        AtlasAgeElement that = (AtlasAgeElement) o;
        return getElementId() == that.getElementId();
    }

    @Override
    public int hashCode() {
        return Long.hashCode(getElementId());
    }
}
