package net.ucanaccess.commands;

import java.sql.SQLException;

public interface ICommand {
    enum TYPES {
        COMPOSITE,
        DDL,
        DELETE,
        INSERT,
        UPDATE
    };

    String getExecId();

    String getTableName();

    TYPES getType();

    IFeedbackAction persist() throws SQLException;

    IFeedbackAction rollback() throws SQLException;
}
