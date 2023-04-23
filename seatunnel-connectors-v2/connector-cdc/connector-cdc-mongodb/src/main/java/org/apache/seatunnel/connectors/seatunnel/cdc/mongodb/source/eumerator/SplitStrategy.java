/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.eumerator;

import io.debezium.relational.TableId;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.bson.BsonDocument;

import java.util.Collection;

/**
 * The {@link MongoDBChunkSplitter} used to split collection into a set of chunks for MongoDB data
 * source.
 */
public interface SplitStrategy {

    Collection<SnapshotSplit> split(SplitContext splitContext);

    default String splitId(TableId collectionId, int chunkId) {
        return collectionId.identifier() + ":" + chunkId;
    }

    default SeaTunnelRowType shardKeysToRowType(BsonDocument shardKeys) {
        return shardKeysToRowType(shardKeys.keySet());
    }

    default SeaTunnelRowType shardKeysToRowType(Collection<String> shardKeys) {
       return null;
    }
}
