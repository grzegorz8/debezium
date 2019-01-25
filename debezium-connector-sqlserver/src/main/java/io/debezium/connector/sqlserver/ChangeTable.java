/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static io.debezium.connector.sqlserver.CaptureInstanceConstants.COL_DATA;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.jdbc.JdbcConnection.ResultSetMapper;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import java.util.Objects;

/**
 * A logical representation of change table containing changes for a given source table.
 * There is usually one change table for each source table. When the schema of the source table
 * is changed then two change tables could be present.
 *
 * @author Jiri Pechanec
 *
 */
public class ChangeTable {

    private static final String CDC_SCHEMA = "cdc";

    /**
     * The logical name of the change capture process
     */
    private final String captureInstance;

    /**
     * The id of table from which the changes are captured
     */
    private final TableId sourceTableId;

    /**
     * The table from which the changes are captured
     */
    private Table sourceTable;

    /**
     * The table that contains the changes for the source table
     */
    private final TableId changeTableId;

    /**
     * A LSN from which the data in the change table are relevant
     */
    private final Lsn startLsn;

    /**
     * A LSN to which the data in the change table are relevant
     */
    private Lsn stopLsn;

    /**
     * Numeric identifier of change table in SQL Server schema
     */
    private final int changeTableObjectId;

    private ResultSetMapper<Object[]> resultSetMapper;

    public ChangeTable(TableId sourceTableId, String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn) {
        super();
        this.sourceTableId = sourceTableId;
        this.captureInstance = captureInstance;
        this.changeTableObjectId = changeTableObjectId;
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;
        this.changeTableId = sourceTableId != null ? new TableId(sourceTableId.catalog(), CDC_SCHEMA, captureInstance + "_CT") : null;
    }

    public ChangeTable(String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn) {
        this(null, captureInstance, changeTableObjectId, startLsn, stopLsn);
    }

    public String getCaptureInstance() {
        return captureInstance;
    }

    public Lsn getStartLsn() {
        return startLsn;
    }

    public Lsn getStopLsn() {
        return stopLsn;
    }

    public void setStopLsn(Lsn stopLsn) {
        this.stopLsn = stopLsn;
    }

    public TableId getSourceTableId() {
        return sourceTableId;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(Table sourceTable) {
        this.sourceTable = sourceTable;
    }

    public TableId getChangeTableId() {
        return changeTableId;
    }

    public int getChangeTableObjectId() {
        return changeTableObjectId;
    }

    @Override
    public String toString() {
        return "Capture instance \"" + captureInstance + "\" [sourceTableId=" + sourceTableId
                + ", changeTableId=" + changeTableId + ", startLsn=" + startLsn + ", changeTableObjectId="
                + changeTableObjectId + ", stopLsn=" + stopLsn + "]";
    }

    /**
     * Internally each row is represented as an array of objects, where the order of values
     * corresponds to the order of columns (fields) in the sourceTable schema. However, when capture
     * instance contains only a subset of original's sourceTable column, in order to preserve the
     * aforementioned order of values in array, raw database results have to be adjusted
     * accordingly.
     *
     * @param resultSet result set
     * @return a mapper which adjusts order of values in case the capture instance contains only
     * a subset of columns
     */
    public ResultSetMapper<Object[]> getResultSetMapper(ResultSet resultSet) throws SQLException {
        if (resultSetMapper == null) {
            resultSetMapper = createResultSetMapper(resultSet);
        }
        return resultSetMapper;
    }

    private ResultSetMapper<Object[]> createResultSetMapper(ResultSet resultSet)
            throws SQLException {
        Objects.requireNonNull(sourceTable);
        final List<String> sourceTableColumns = sourceTable.columnNames();
        final List<String> resultColumns = getResultColumnNames(resultSet);
        final int sourceColumnCount = sourceTableColumns.size();
        final int resultColumnCount = resultColumns.size();

        if (sourceTableColumns.equals(resultColumns)) {
            return rs -> {
                final Object[] data = new Object[sourceColumnCount];
                for (int i = 0; i < sourceColumnCount; i++) {
                    data[i] = rs.getObject(COL_DATA + i);
                }
                return data;
            };
        }
        else {
            final IndicesMapping indicesMapping = new IndicesMapping(sourceTableColumns, resultColumns);
            return rs -> {
                final Object[] data = new Object[sourceColumnCount];
                for (int i = 0; i < resultColumnCount; i++) {
                    int index = indicesMapping.getSourceTableColumnIndex(i);
                    data[index] = rs.getObject(COL_DATA + i);
                }
                return data;
            };
        }
    }

    private List<String> getResultColumnNames(ResultSet resultSet) throws SQLException {
        final int columnCount = resultSet.getMetaData().getColumnCount() - (COL_DATA - 1);
        final List<String> columns = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; ++i) {
            columns.add(resultSet.getMetaData().getColumnName(COL_DATA + i));
        }
        return columns;
    }

    private static class IndicesMapping {

        private final Map<Integer, Integer> mapping;

        IndicesMapping(List<String> sourceTableColumns, List<String> captureInstanceColumns) {
            this.mapping = new HashMap<>(sourceTableColumns.size());

            for (int i = 0; i < captureInstanceColumns.size(); ++i) {
                mapping.put(i, sourceTableColumns.indexOf(captureInstanceColumns.get(i)));
            }

        }

        int getSourceTableColumnIndex(int resultCaptureInstanceColumnIndex) {
            return mapping.get(resultCaptureInstanceColumnIndex);
        }
    }
}
