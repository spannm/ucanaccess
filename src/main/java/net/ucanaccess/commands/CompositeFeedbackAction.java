/*
 Copyright (c) 2012 Marco Amadei.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package net.ucanaccess.commands;

import java.sql.SQLException;
import java.util.ArrayList;

public class CompositeFeedbackAction implements IFeedbackAction {
    private ArrayList<IFeedbackAction> actions = new ArrayList<IFeedbackAction>();

    @Override
    public void doAction(ICommand toChange) throws SQLException {
        for (IFeedbackAction action : actions) {
            action.doAction(toChange);
        }

    }

    public boolean add(IFeedbackAction ifa) {
        if (ifa == null) {
            return false;
        }
        return actions.add(ifa);
    }

}
