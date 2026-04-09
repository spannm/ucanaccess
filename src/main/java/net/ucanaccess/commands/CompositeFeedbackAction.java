package net.ucanaccess.commands;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Composite pattern implementation for {@link IFeedbackAction}.
 * <p>
 * Holds a list of feedback actions to be executed sequentially.
 */
public class CompositeFeedbackAction implements IFeedbackAction {
    private final List<IFeedbackAction> actions = new ArrayList<>();

    @Override
    public void doAction(ICommand toChange) throws SQLException {
        for (IFeedbackAction action : actions) {
            action.doAction(toChange);
        }
    }

    /**
     * Adds a feedback action to the composite.
     *
     * @param ifa the action to add
     * @return true if the action was added, false otherwise
     */
    public boolean add(IFeedbackAction ifa) {
        return ifa != null && actions.add(ifa);
    }

    @Override
    public String toString() {
        return String.format("%s[actions=%s]", getClass().getSimpleName(), actions);
    }

}

