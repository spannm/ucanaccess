package net.ucanaccess.jdbc;

public class Session {
    private String password;
    private String user;

    public String getPassword() {
        return password;
    }

    public String getUser() {
        return user;
    }

    public void setPassword(String _password) {
        this.password = _password;
    }

    public void setUser(String _user) {
        this.user = _user;
    }
}
