/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static io.debezium.connector.sqlserver.CaptureInstanceConstants.COL_COMMIT_LSN;
import static io.debezium.connector.sqlserver.CaptureInstanceConstants.COL_OPERATION;
import static io.debezium.connector.sqlserver.CaptureInstanceConstants.COL_ROW_LSN;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The logical representation of a position for the change in the transaction log.
 * During each sourcing cycle it is necessary to query all change tables and then
 * make a total order of changes across all tables.<br>
 * This class represents an open database cursor over the change table that is
 * able to move the cursor forward and report the LSN for the change to which the cursor
 * now points.
 *
 * @author Jiri Pechanec
 *
 */
public class ChangeTablePointer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeTablePointer.class);

    private final ChangeTable changeTable;
    private final ResultSet resultSet;
    private boolean completed = false;
    private TxLogPosition currentChangePosition;

    public ChangeTablePointer(ChangeTable changeTable, ResultSet resultSet) {
        this.changeTable = changeTable;
        this.resultSet = resultSet;
    }

    public ChangeTable getChangeTable() {
        return changeTable;
    }

    public TxLogPosition getChangePosition() {
        return currentChangePosition;
    }

    public int getOperation() throws SQLException {
        return resultSet.getInt(COL_OPERATION);
    }

    public Object[] getData() throws SQLException {
        return changeTable.getResultSetMapper(resultSet).apply(resultSet);
    }

    public boolean next() throws SQLException {
        completed = !resultSet.next();
        currentChangePosition = completed ? TxLogPosition.NULL : TxLogPosition.valueOf(Lsn.valueOf(resultSet.getBytes(COL_COMMIT_LSN)), Lsn.valueOf(resultSet.getBytes(COL_ROW_LSN)));
        if (completed) {
            LOGGER.trace("Closing result set of change tables for table {}", changeTable);
            resultSet.close();
        }
        return !completed;
    }

    public boolean isCompleted() {
        return completed;
    }

    public int compareTo(ChangeTablePointer o) {
        return getChangePosition().compareTo(o.getChangePosition());
    }

    @Override
    public String toString() {
        return "ChangeTablePointer [changeTable=" + changeTable + ", resultSet=" + resultSet + ", completed="
                + completed + ", currentChangePosition=" + currentChangePosition + "]";
    }

}
