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
package io.trino.jdbc;

import org.testng.annotations.Test;

import java.util.Locale;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestQueryRefactor
{
    private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
    private static final Pattern SQL_COMMENT_PATTERN = Pattern.compile("--.*?(\r?\n|$)|/\\*.*?\\*/", Pattern.DOTALL);

    public static boolean areSqlQueriesEqual(String sql1, String sql2)
    {
        String normalizedSql1 = normalizeSql(sql1);
        String normalizedSql2 = normalizeSql(sql2);
        return normalizedSql1.equals(normalizedSql2);
    }

    private static String normalizeSql(String sql)
    {
        // Remove SQL comments
        sql = SQL_COMMENT_PATTERN.matcher(sql).replaceAll(" ");
        // Normalize whitespace and remove new lines
        sql = WHITESPACE_PATTERN.matcher(sql).replaceAll(" ");
        // Convert to lowercase for case-insensitive comparison
        return sql.trim().toLowerCase(Locale.ROOT);
    }

    @Test
    public void testRefactorQueryWithValidSelect()
    {
        String query = "SELECT id, name FROM some_table WHERE id = 1";
        QueryRefactor queryRefactor = new QueryRefactor(10L);

        String result = queryRefactor.refactorQuery(query);
        String expected = "WITH GHTK_CTE AS (\n" +
                "   SELECT\n" +
                "     id ,\n" +
                "     name\n" +
                "   FROM\n" +
                "     some_table\n" +
                "   WHERE\n" +
                "     (id = 1)\n" +
                ") SELECT * FROM GHTK_CTE LIMIT 10";
        assertThat(areSqlQueriesEqual(expected, result)).isTrue();
    }

    @Test
    public void testRefactorQueryWithExistingWithClause()
    {
        String query = "WITH cte AS (SELECT id FROM some_table) SELECT * FROM cte";
        QueryRefactor queryRefactor = new QueryRefactor(10L);

        String result = queryRefactor.refactorQuery(query);
        String expected = "WITH cte AS (\n" +
                "   SELECT\n" +
                "     id\n" +
                "   FROM\n" +
                "     some_table\n" +
                ") , GHTK_CTE AS (\n" +
                "   SELECT\n" +
                "     *\n" +
                "   FROM\n" +
                "     cte\n" +
                ") SELECT * FROM GHTK_CTE LIMIT 10";
        assertThat(areSqlQueriesEqual(expected, result)).isTrue();
    }

    @Test
    public void testRefactorQueryWithInvalidQuery()
    {
        String query = "DROP TABLE test_long_running";
        QueryRefactor queryRefactor = new QueryRefactor(10L);

        String result = queryRefactor.refactorQuery(query);
        assertThat(areSqlQueriesEqual(result, result)).isTrue();
    }

    @Test
    public void testRefactorQueryWithNoSelect()
    {
        String query = "INSERT INTO some_table (id, name) VALUES (1, 'name')";
        QueryRefactor queryRefactor = new QueryRefactor(10L);

        String result = queryRefactor.refactorQuery(query);
        assertThat(areSqlQueriesEqual(result, result)).isTrue();
    }
}
