package net.ucanaccess.commands;

import com.healthmarketscience.jackcess.Cursor;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public interface ICursorCommand extends ICommand {
    boolean currentRowMatches(Cursor cur, Map<String, Object> currentRow);

    IndexSelector getIndexSelector();

    Map<String, Object> getRowPattern();

    IFeedbackAction persistCurrentRow(Cursor cur) throws IOException, SQLException;
}
