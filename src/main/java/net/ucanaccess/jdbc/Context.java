package net.ucanaccess.jdbc;

public class Context {
    private UcanaccessConnection currentConnection;
    private String               currentExecId;

    public Context(UcanaccessConnection currentConnection) {
        this.currentConnection = currentConnection;
    }

    public UcanaccessConnection getCurrentConnection() {
        return currentConnection;
    }

    public String getCurrentExecId() {
        return currentExecId;
    }

    public void setCurrentConnection(UcanaccessConnection currentConnection) {
        this.currentConnection = currentConnection;
    }

    public void setCurrentExecId(String currentExecId) {
        this.currentExecId = currentExecId;
    }

}
