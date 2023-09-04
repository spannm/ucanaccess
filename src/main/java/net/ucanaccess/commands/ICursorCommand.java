package net.ucanaccess.commands;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import com.healthmarketscience.jackcess.Cursor;

public interface ICursorCommand extends ICommand {
    boolean currentRowMatches(Cursor cur, Map<String, Object> currentRow) throws IOException;

    IndexSelector getIndexSelector();

    Map<String, Object> getRowPattern();

    IFeedbackAction persistCurrentRow(Cursor cur) throws IOException, SQLException;
}
