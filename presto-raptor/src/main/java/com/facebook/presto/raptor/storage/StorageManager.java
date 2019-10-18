/*
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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.filesystem.FileSystemContext;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;

public interface StorageManager
{
    default ConnectorPageSource getPageSource(
            FileSystemContext fileSystemContext,
            UUID shardUuid,
            OptionalInt bucketNumber,
            List<Long> columnIds,
            List<Type> columnTypes,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            ReaderAttributes readerAttributes)
    {
        return getPageSource(fileSystemContext, shardUuid, bucketNumber, columnIds, columnTypes, effectivePredicate, readerAttributes, OptionalLong.empty(), Optional.empty());
    }

    ConnectorPageSource getPageSource(
            FileSystemContext fileSystemContext,
            UUID shardUuid,
            OptionalInt bucketNumber,
            List<Long> columnIds,
            List<Type> columnTypes,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            ReaderAttributes readerAttributes,
            OptionalLong transactionId,
            Optional<Map<String, Type>> allColumnTypes);

    StoragePageSink createStoragePageSink(
            FileSystemContext fileSystemContext,
            long transactionId,
            OptionalInt bucketNumber,
            List<Long> columnIds,
            List<Type> columnTypes,
            boolean checkSpace);
}
