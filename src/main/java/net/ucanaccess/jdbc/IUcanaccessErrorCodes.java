package net.ucanaccess.jdbc;

import org.hsqldb.error.ErrorCode;

@SuppressWarnings({"PMD.ConstantsInInterface", "checkstyle:InterfaceIsType"}) // public API, extends the HSQLDB error codes
public interface IUcanaccessErrorCodes extends ErrorCode {
    int UCANACCESS_GENERIC_ERROR = 15000; // Ucanaccess Error
}
