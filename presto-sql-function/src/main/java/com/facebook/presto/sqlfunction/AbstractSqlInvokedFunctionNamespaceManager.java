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
package com.facebook.presto.sqlfunction;

import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.QualifiedFunctionName;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.facebook.presto.spi.function.SqlParameter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.GuardedBy;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.function.FunctionImplementationType.SQL;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class AbstractSqlInvokedFunctionNamespaceManager
        implements FunctionNamespaceManager<SqlInvokedFunction>
{
    private final ConcurrentMap<FunctionNamespaceTransactionHandle, FunctionCollection> transactions = new ConcurrentHashMap<>();

    private final LoadingCache<QualifiedFunctionName, Collection<SqlInvokedFunction>> functions;
    private final LoadingCache<SqlFunctionHandle, FunctionMetadata> metadataByHandle;
    private final LoadingCache<SqlFunctionHandle, ScalarFunctionImplementation> implementationByHandle;

    public AbstractSqlInvokedFunctionNamespaceManager(SqlInvokedFunctionNamespaceManagerConfig config)
    {
        this.functions = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<QualifiedFunctionName, Collection<SqlInvokedFunction>>()
                {
                    @Override
                    @ParametersAreNonnullByDefault
                    public Collection<SqlInvokedFunction> load(QualifiedFunctionName functionName)
                    {
                        Collection<SqlInvokedFunction> functions = fetchFunctionsDirect(functionName);
                        for (SqlInvokedFunction function : functions) {
                            metadataByHandle.put(function.getRequiredFunctionHandle(), sqlInvokedFunctionToMetadata(function));
                        }
                        return functions;
                    }
                });
        this.metadataByHandle = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionInstanceCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<SqlFunctionHandle, FunctionMetadata>()
                {
                    @Override
                    @ParametersAreNonnullByDefault
                    public FunctionMetadata load(SqlFunctionHandle functionHandle)
                    {
                        return fetchFunctionMetadataDirect(functionHandle);
                    }
                });
        this.implementationByHandle = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionInstanceCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<SqlFunctionHandle, ScalarFunctionImplementation>()
                {
                    @Override
                    public ScalarFunctionImplementation load(SqlFunctionHandle functionHandle)
                    {
                        return fetchFunctionImplementationDirect(functionHandle);
                    }
                });
    }

    protected abstract Collection<SqlInvokedFunction> fetchFunctionsDirect(QualifiedFunctionName functionName);

    protected abstract FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle);

    protected abstract ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlFunctionHandle functionHandle);

    @Override
    public final FunctionNamespaceTransactionHandle beginTransaction()
    {
        UuidFunctionNamespaceTransactionHandle transactionHandle = UuidFunctionNamespaceTransactionHandle.create();
        transactions.put(transactionHandle, new FunctionCollection());
        return transactionHandle;
    }

    @Override
    public final void commit(FunctionNamespaceTransactionHandle transactionHandle)
    {
        // Transactional commit is not supported yet.
        transactions.remove(transactionHandle);
    }

    @Override
    public final void abort(FunctionNamespaceTransactionHandle transactionHandle)
    {
        // Transactional rollback is not supported yet.
        transactions.remove(transactionHandle);
    }

    @Override
    public final Collection<SqlInvokedFunction> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedFunctionName functionName)
    {
        checkArgument(transactionHandle.isPresent(), "missing transactionHandle");
        return transactions.get(transactionHandle.get()).loadAndGetFunctionsTransactional(functionName);
    }

    @Override
    public final FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        checkArgument(transactionHandle.isPresent(), "missing transactionHandle");
        // This is the only assumption in this class that we're dealing with sql-invoked regular function.
        SqlFunctionId functionId = new SqlFunctionId(signature.getName(), signature.getArgumentTypes());
        return transactions.get(transactionHandle.get()).getFunctionHandle(functionId);
    }

    @Override
    public final FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof SqlFunctionHandle, "Unsupported FunctionHandle type '%s'", functionHandle.getClass().getSimpleName());
        return metadataByHandle.getUnchecked((SqlFunctionHandle) functionHandle);
    }

    @Override
    public final ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof SqlFunctionHandle, "Unsupported FunctionHandle type '%s'", functionHandle.getClass().getSimpleName());
        return implementationByHandle.getUnchecked((SqlFunctionHandle) functionHandle);
    }

    protected void refreshFunctionsCache(QualifiedFunctionName functionName)
    {
        functions.refresh(functionName);
    }

    protected static FunctionMetadata sqlInvokedFunctionToMetadata(SqlInvokedFunction function)
    {
        return new FunctionMetadata(
                function.getSignature().getName(),
                function.getSignature().getArgumentTypes(),
                function.getParameters().stream()
                        .map(SqlParameter::getName)
                        .collect(toImmutableList()),
                function.getSignature().getReturnType(),
                SCALAR,
                function.getFunctionImplementationType(),
                function.isDeterministic(),
                function.isCalledOnNullInput());
    }

    protected static ScalarFunctionImplementation sqlInvokedFunctionToImplementation(SqlInvokedFunction function)
    {
        checkArgument(function.getFunctionImplementationType().equals(SQL));
        return new SqlInvokedScalarFunctionImplementation(function.getBody());
    }

    private Collection<SqlInvokedFunction> fetchFunctions(QualifiedFunctionName functionName)
    {
        return functions.getUnchecked(functionName);
    }

    private class FunctionCollection
    {
        @GuardedBy("this")
        private final Map<QualifiedFunctionName, Collection<SqlInvokedFunction>> functions = new ConcurrentHashMap<>();

        @GuardedBy("this")
        private final Map<SqlFunctionId, SqlFunctionHandle> functionHandles = new ConcurrentHashMap<>();

        public synchronized Collection<SqlInvokedFunction> loadAndGetFunctionsTransactional(QualifiedFunctionName functionName)
        {
            Collection<SqlInvokedFunction> functions = this.functions.computeIfAbsent(functionName, AbstractSqlInvokedFunctionNamespaceManager.this::fetchFunctions);
            functionHandles.putAll(functions.stream().collect(toImmutableMap(SqlInvokedFunction::getFunctionId, SqlInvokedFunction::getRequiredFunctionHandle)));
            return functions;
        }

        public synchronized FunctionHandle getFunctionHandle(SqlFunctionId functionId)
        {
            return functionHandles.get(functionId);
        }
    }
}
