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
package com.facebook.presto.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class PrestoPreparedStatement
        extends PrestoStatement
        implements PreparedStatement
{
    private String sql;

    PrestoPreparedStatement(PrestoConnection connection, String sql)
            throws SQLException
    {
        super(connection);
        this.sql = sql;
    }

    @Override
    public ResultSet executeQuery()
            throws SQLException
    {
        sql = sql.replaceAll("([?])", "null");
        return super.executeQuery(sql);
    }

    @Override
    public int executeUpdate()
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "executeUpdate");
    }

    private String replaceQueryParam(String sql, String value)
    {
        return sql.replaceFirst("([?])", value);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setNull");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setBoolean");
    }

    @Override
    public void setByte(int parameterIndex, byte x)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setByte");
    }

    @Override
    public void setShort(int parameterIndex, short x)
            throws SQLException
    {
        sql = replaceQueryParam(sql, Integer.toString(x));
    }

    @Override
    public void setInt(int parameterIndex, int x)
            throws SQLException
    {
        sql = replaceQueryParam(sql, Integer.toString(x));
    }

    @Override
    public void setLong(int parameterIndex, long x)
            throws SQLException
    {
        sql = replaceQueryParam(sql, Long.toString(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x)
            throws SQLException
    {
        sql = replaceQueryParam(sql, Float.toString(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x)
            throws SQLException
    {
        sql = replaceQueryParam(sql, Double.toString(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException
    {
        sql = replaceQueryParam(sql, x.toString());
    }

    @Override
    public void setString(int parameterIndex, String x)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setString");
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setBytes");
    }

    @Override
    public void setDate(int parameterIndex, Date x)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setDate");
    }

    @Override
    public void setTime(int parameterIndex, Time x)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setTime");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setTimestamp");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setAsciiStream");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setUnicodeStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setBinaryStream");
    }

    @Override
    public void clearParameters()
            throws SQLException
    {
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setObject");
    }

    @Override
    public void setObject(int parameterIndex, Object x)
            throws SQLException
    {
        if (x instanceof String) {
            sql = sql.replaceFirst("([?])", "'" + x.toString() + "'");
        }
        else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal) x);
        }
        else if (x instanceof Short) {
            setShort(parameterIndex, (Short) x);
        }
        else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        }
        else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        }
        else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        }
        else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        }
    }

    @Override
    public boolean execute()
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "execute");
    }

    @Override
    public void addBatch()
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "addBatch");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setCharacterStream");
    }

    @Override
    public void setRef(int parameterIndex, Ref x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setRef");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setClob(int parameterIndex, Clob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setArray(int parameterIndex, Array x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setArray");
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getMetaData");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setDate");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setTime");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setTimestamp");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setNull");
    }

    @Override
    public void setURL(int parameterIndex, URL x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setURL");
    }

    @Override
    public ParameterMetaData getParameterMetaData()
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "getParameterMetaData");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setRowId");
    }

    @Override
    public void setNString(int parameterIndex, String value)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNString");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNCharacterStream");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setSQLXML");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setObject");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setCharacterStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setCharacterStream");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNCharacterStream");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void addBatch(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }
}
