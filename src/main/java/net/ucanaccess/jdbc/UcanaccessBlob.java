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

    public UcanaccessBlob(Blob blob, UcanaccessConnection conn) throws SQLException {
        this.blob = blob;
        this.conn = conn;
        if (blob.length() != 0) {
            BlobKey bk = BlobKey.getBlobKey(blob.getBinaryStream());
            usingBlobKey = bk != null;
            if (usingBlobKey) {
                this.blob = bk.getOleBlob(this.conn.getDbIO());
            }
        }
    }

    public static Blob createBlob(File fl, UcanaccessConnection conn) throws UcanaccessSQLException {
        return Try.catching(() -> {
            Blob oleBlob = new OleBlob.Builder().withPackagePrettyName(fl.getName()).withSimplePackage(fl).toBlob();
            return new UcanaccessBlob(oleBlob, conn);
        }).orThrow(UcanaccessSQLException::new);
    }

    public static Blob createBlob(UcanaccessConnection conn) throws SQLException {
        return new UcanaccessBlob(conn.getHSQLDBConnection().createBlob(), conn);
    }

    @Override
    public void free() throws SQLException {
        try {
            blob.free();
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
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
        } catch (SQLException | IOException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        try {
            return blob.getBinaryStream(pos, length);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        try {
            return blob.getBytes(pos, length);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public long length() throws SQLException {
        try {
            return blob.length();
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        try {
            return blob.position(pattern, start);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        try {
            return blob.position(pattern, start);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
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
        } catch (SQLException | IOException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        try {
            return blob.setBytes(pos, bytes);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        try {
            return blob.setBytes(pos, bytes, offset, len);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

    @Override
    public void truncate(long len) throws SQLException {
        try {
            blob.truncate(len);
        } catch (SQLException ex) {
            throw new UcanaccessSQLException(ex);
        }
    }

}
