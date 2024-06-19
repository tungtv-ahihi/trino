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

import com.google.common.collect.ImmutableList;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.util.List;
import java.util.Optional;

public class QueryRefactor
{
    private final SqlParser sqlParser;

    private final Long limitRows;

    public QueryRefactor(Long limitRows)
    {
        this(new SqlParser(), limitRows);
    }

    public QueryRefactor(SqlParser sqlParser, Long limitRows)
    {
        this.sqlParser = sqlParser;
        this.limitRows = limitRows;
    }

    public String refactorQuery(String query)
    {
        Statement stmt = sqlParser.createStatement(query, new ParsingOptions());

        if (stmt instanceof Query) {
            try {
                Query queryStmt = (Query) stmt;
                return wrapCte(queryStmt);
            }
            catch (IllegalAccessException e) {
                e.printStackTrace();
                return "Error updating WITH clause";
            }
        }
        else {
            return query;
        }
    }

    private String wrapCte(Query queryStmt)
            throws IllegalAccessException
    {
        String formattedQueryBody = formatQueryBody(queryStmt.getQueryBody());
        String queryBodyToWith = createWithQuery(formattedQueryBody);
        Statement newWithStmt = sqlParser.createStatement(queryBodyToWith, new ParsingOptions());
        List<WithQuery> combinedWithQueries = combineWithQueries(queryStmt, newWithStmt);
        updateWithClause(newWithStmt, combinedWithQueries);

        return SqlFormatter.formatSql(newWithStmt);
    }

    private String formatQueryBody(Node queryStmt)
    {
        return SqlFormatter.formatSql(queryStmt);
    }

    private String createWithQuery(String formattedQueryBody)
    {
        return "WITH GHTK_CTE AS (" + formattedQueryBody
                + ") SELECT * FROM GHTK_CTE LIMIT "
                + limitRows.toString();
    }

    private List<WithQuery> combineWithQueries(Query originalQuery, Statement newWithStmt)
    {
        List<WithQuery> oldWithQueries = originalQuery.getWith().map(With::getQueries).orElseGet(ImmutableList::of);
        List<WithQuery> newWithQueries = ((Query) newWithStmt).getWith().map(With::getQueries).orElseGet(ImmutableList::of);

        return ImmutableList.<WithQuery>builder()
                .addAll(oldWithQueries)
                .addAll(newWithQueries)
                .build();
    }

    private void updateWithClause(Statement newWithStmt, List<WithQuery> combinedWithQueries)
            throws IllegalAccessException
    {
        Optional<With> with = Optional.of(new With(false, combinedWithQueries));
        FieldUtils.writeField(newWithStmt, "with", with, true);
    }
}
