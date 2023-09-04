package net.ucanaccess.jdbc;

public class Context {
    private UcanaccessConnection currentConnection;
    private String               currentExecId;

    public Context(UcanaccessConnection _currentConnection) {
        this.currentConnection = _currentConnection;
    }

    public UcanaccessConnection getCurrentConnection() {
        return currentConnection;
    }

    public String getCurrentExecId() {
        return currentExecId;
    }

    public void setCurrentConnection(UcanaccessConnection _currentConnection) {
        this.currentConnection = _currentConnection;
    }

    public void setCurrentExecId(String _currentExecId) {
        this.currentExecId = _currentExecId;
    }

}
