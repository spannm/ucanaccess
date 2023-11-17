package net.ucanaccess.commands;

import java.sql.SQLException;

public interface ICommand {
    enum CommandType {
        COMPOSITE,
        DDL,
        DELETE,
        INSERT,
        UPDATE
    }

    String getExecId();

    String getTableName();

    CommandType getType();

    IFeedbackAction persist() throws SQLException;

    IFeedbackAction rollback() throws SQLException;
}
