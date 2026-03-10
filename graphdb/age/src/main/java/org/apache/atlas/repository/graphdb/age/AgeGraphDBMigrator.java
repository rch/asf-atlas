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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.MigrationStatus;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graphdb.GraphDBMigrator;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.springframework.stereotype.Component;

import java.io.InputStream;

/**
 * No-op GraphDBMigrator for Apache AGE backend.
 *
 * GraphSON migration is not supported with AGE — data is managed
 * directly in PostgreSQL via the AGE extension.
 */
@Component
public class AgeGraphDBMigrator implements GraphDBMigrator {

    @Override
    public AtlasTypesDef getScrubbedTypesDef(String jsonStr) {
        return AtlasType.fromJson(jsonStr, AtlasTypesDef.class);
    }

    @Override
    public void importData(AtlasTypeRegistry typeRegistry, InputStream fs) throws AtlasBaseException {
        throw new AtlasBaseException("Data migration via GraphSON import is not supported with the AGE backend");
    }

    @Override
    public MigrationStatus getMigrationStatus() {
        return null;
    }
}
