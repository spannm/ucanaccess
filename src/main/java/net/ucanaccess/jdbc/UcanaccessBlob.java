package net.ucanaccess.jdbc;

import io.github.spannm.jackcess.util.OleBlob;
import io.github.spannm.jackcess.util.OleBlob.Content;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.util.Try;

import java.io.*;
import java.sql.Blob;
import java.sql.SQLException;

public class UcanaccessBlob implements Blob {
    private Blob                       blob;
    private boolean                    usingBlobKey;
    private final UcanaccessConnection conn;

    public UcanaccessBlob(Blob _blob, UcanaccessConnection _conn) throws SQLException {
        blob = _blob;
        conn = _conn;
        if (_blob.length() != 0) {
            BlobKey bk = BlobKey.getBlobKey(_blob.getBinaryStream());
            usingBlobKey = bk != null;
            if (usingBlobKey) {
                blob = bk.getOleBlob(conn.getDbIO());
            }
        }
    }

    public static Blob createBlob(File fl, UcanaccessConnection _conn) throws UcanaccessSQLException {
        return Try.catching(() -> {
            Blob oleBlob = new OleBlob.Builder().withPackagePrettyName(fl.getName()).withSimplePackage(fl).toBlob();
            return new UcanaccessBlob(oleBlob, _conn);
        }).orThrow(UcanaccessSQLException::new);
    }

    public static Blob createBlob(UcanaccessConnection _conn) throws SQLException {
        return new UcanaccessBlob(_conn.getHSQLDBConnection().createBlob(), _conn);
    }

    @Override
    public void free() throws SQLException {
        try {
            blob.free();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        try {
            if (usingBlobKey) {
                OleBlob ole = (OleBlob) blob;
                if (ole.getContent() instanceof OleBlob.EmbeddedContent) {
                    return ((OleBlob.EmbeddedContent) ole.getContent()).getStream();
                }
            }
            return blob.getBinaryStream();
        } catch (SQLException | IOException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        try {
            return blob.getBinaryStream(pos, length);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        try {
            return blob.getBytes(pos, length);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public long length() throws SQLException {
        try {
            return blob.length();
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        try {
            return blob.position(pattern, start);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        try {
            return blob.position(pattern, start);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        try {
            if (blob instanceof OleBlob && pos == 1) {
                OleBlob ole = (OleBlob) blob;
                Content content = ole.getContent();
                if (content instanceof OleBlob.EmbeddedContent) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ((OleBlob.SimplePackageContent) content).writeTo(baos);
                    return baos;
                }
            }
            return blob.setBinaryStream(pos);
        } catch (SQLException | IOException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        try {
            return blob.setBytes(pos, bytes);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        try {
            return blob.setBytes(pos, bytes, offset, len);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

    @Override
    public void truncate(long len) throws SQLException {
        try {
            blob.truncate(len);
        } catch (SQLException _ex) {
            throw new UcanaccessSQLException(_ex);
        }
    }

}
