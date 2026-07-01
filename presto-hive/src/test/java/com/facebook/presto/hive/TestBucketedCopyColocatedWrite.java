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
package com.facebook.presto.hive;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.REDISTRIBUTE_WRITES;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertTrue;

/**
 * Reproduction / regression test for the "bucketed -> bucketed copy shuffles" inefficiency:
 * {@code INSERT INTO dst SELECT ... FROM src} where src and dst are bucketed identically (same
 * column, same bucket_count) should write colocated (read bucket b -> write bucket b, no shuffle),
 * because the source is already partitioned exactly the way the writer needs.
 *
 * The check: in the distributed plan, the fragment that contains the {@code TableWriter} should
 * also contain the {@code TableScan} of the source table (same fragment == colocated == no shuffle).
 * If instead a repartition exchange is inserted, the writer's fragment sees a {@code RemoteSource}
 * and the source scan lives in a separate fragment -> the test fails and prints the plan.
 *
 * NOTE: uses the distributed HiveQueryRunner, so it must run in CI / on a devserver (the in-process
 * runner cannot start on a laptop -- DiscoveryException).
 */
@Test(singleThreaded = true)
public class TestBucketedCopyColocatedWrite
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestBucketedCopyColocatedWrite.class);

    @Language("SQL")
    private static final String EXPLAIN_COPY =
            "EXPLAIN (TYPE DISTRIBUTED) INSERT INTO bcopy_dst SELECT k, v FROM bcopy_src";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(getTables());
    }

    @BeforeClass
    public void setUp()
    {
        assertUpdate("CREATE TABLE bcopy_src (k BIGINT, v BIGINT) WITH (bucketed_by = ARRAY['k'], bucket_count = 16)");
        assertUpdate("CREATE TABLE bcopy_dst (k BIGINT, v BIGINT) WITH (bucketed_by = ARRAY['k'], bucket_count = 16)");
        // Populate the source so the optimizer has realistic stats.
        getQueryRunner().execute(getSession(), "INSERT INTO bcopy_src SELECT orderkey, custkey FROM orders");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertUpdate("DROP TABLE IF EXISTS bcopy_src");
        assertUpdate("DROP TABLE IF EXISTS bcopy_dst");
    }

    @Test
    public void bucketedToBucketedCopyShouldBeColocated_defaultSession()
    {
        assertCopyIsColocated(getSession(), "default");
    }

    @Test
    public void bucketedToBucketedCopyShouldBeColocated_redistributeWritesOff()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(REDISTRIBUTE_WRITES, "false")
                .build();
        assertCopyIsColocated(session, "redistribute_writes=false");
    }

    private void assertCopyIsColocated(Session session, String label)
    {
        String plan = (String) computeActual(session, EXPLAIN_COPY).getOnlyValue();
        log.info("[%s] bucketed->bucketed copy distributed plan:%n%s", label, plan);
        assertTrue(
                writerAndSourceScanInSameFragment(plan, "bcopy_src"),
                "[" + label + "] bucketed->bucketed copy inserted a repartition shuffle before the writer "
                        + "(writer fragment does not contain the source scan). Plan:\n" + plan);
    }

    /**
     * True iff the fragment containing the actual {@code TableWriter[} operator also contains a
     * {@code TableScan} of {@code srcTable} -- i.e. read and write are colocated in one fragment,
     * with no repartition exchange between them.
     */
    private static boolean writerAndSourceScanInSameFragment(String distributedPlan, String srcTable)
    {
        for (String fragment : distributedPlan.split("(?=Fragment )")) {
            // "TableWriter[" matches the writer operator but not "TableWriterMerge[" (the commit side).
            if (fragment.contains("TableWriter[")) {
                return fragment.contains("TableScan") && fragment.contains(srcTable);
            }
        }
        return false;
    }
}
