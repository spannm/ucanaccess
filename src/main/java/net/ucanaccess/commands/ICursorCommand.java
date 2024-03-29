package net.ucanaccess.commands;

import io.github.spannm.jackcess.Cursor;

import java.io.IOException;
import java.util.Map;

public interface ICursorCommand extends ICommand {
    boolean currentRowMatches(Cursor cur, Map<String, Object> currentRow);

    IndexSelector getIndexSelector();

    Map<String, Object> getRowPattern();

    IFeedbackAction persistCurrentRow(Cursor cur) throws IOException;
}
