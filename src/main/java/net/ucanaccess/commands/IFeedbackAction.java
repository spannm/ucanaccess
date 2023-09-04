package net.ucanaccess.commands;

import java.sql.SQLException;

public interface IFeedbackAction {

    void doAction(ICommand toChange) throws SQLException;

}
