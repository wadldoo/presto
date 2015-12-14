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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import com.facebook.presto.spi.type.Type;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;

import org.postgresql.Driver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

public class PostgreSqlClient
        extends BaseJdbcClient
{
    @Inject
    public PostgreSqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
            throws SQLException
    {
        super(connectorId, config, "\"", new Driver());
    }

    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle, Collection<Slice> fragments)
    {
        // PostgreSQL does not allow qualifying the target of a rename
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()))
                .append(" RENAME TO ")
                .append(quoted(handle.getTableName()));

        try (Connection connection = getConnection(handle)) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Statement getStatement(Connection connection)
            throws SQLException
    {
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    protected Type toPrestoType(int jdbcType)
    {
        switch (jdbcType) {
          case Types.BIT:
          case Types.BOOLEAN:
            return BOOLEAN;
          case Types.TINYINT:
          case Types.SMALLINT:
          case Types.INTEGER:
          case Types.BIGINT:
            return BIGINT;
          case Types.FLOAT:
          case Types.REAL:
          case Types.DOUBLE:
          case Types.NUMERIC:
          case Types.DECIMAL:
            return DOUBLE;
          case Types.CHAR:
          case Types.NCHAR:
          case Types.VARCHAR:
          case Types.NVARCHAR:
          case Types.LONGVARCHAR:
          case Types.LONGNVARCHAR:
            return VARCHAR;
          case Types.BINARY:
          case Types.VARBINARY:
          case Types.LONGVARBINARY:
            return VARBINARY;
          case Types.DATE:
            return DATE;
          case Types.TIME:
            return TIME;
          case Types.TIMESTAMP:
            return TIMESTAMP;
          default:
            return VARBINARY;
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
        throws SQLException
    {
      DatabaseMetaData metadata = connection.getMetaData();
      String escape = metadata.getSearchStringEscape();
      return metadata.getTables(
          connection.getCatalog(),
          escapeNamePattern(schemaName, escape),
          escapeNamePattern(tableName, escape),
          new String[] {"TABLE", "VIEW"});
    }
}
