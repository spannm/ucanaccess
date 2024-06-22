package net.ucanaccess.commands;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CompositeFeedbackAction implements IFeedbackAction {
    private final List<IFeedbackAction> actions = new ArrayList<>();

    @Override
    public void doAction(ICommand toChange) throws SQLException {
        for (IFeedbackAction action : actions) {
            action.doAction(toChange);
        }

    }

    public boolean add(IFeedbackAction ifa) {
        return ifa != null && actions.add(ifa);
    }

}
