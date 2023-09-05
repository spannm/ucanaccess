package net.ucanaccess.jdbc;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

public class UcanaccessDataSourceFactory implements ObjectFactory {

    @Override
    public Object getObjectInstance(Object uref, Name name, Context nameCtx, Hashtable<?, ?> environment) {

        String dataSourceClass = UcanaccessDataSource.class.getName();
        Reference ref = (Reference) uref;
        if (ref.getClassName().equals(dataSourceClass)) {
            UcanaccessDataSource dataSource = new UcanaccessDataSource();
            dataSource.setAccessPath((String) ref.get("accessPath").getContent());
            dataSource.setUser((String) ref.get("user").getContent());
            dataSource.setPassword((String) ref.get("password").getContent());

            dataSource.setColumnOrder((String) ref.get("columnorder").getContent());
            dataSource.setConcatNulls((Boolean) ref.get("concatnulls").getContent());
            dataSource.setEncrypt((Boolean) ref.get("encrypt").getContent());
            dataSource.setIgnoreCase((Boolean) ref.get("ignorecase").getContent());
            dataSource.setImmediatelyReleaseResources((Boolean) ref.get("immediatelyreleaseresources").getContent());
            dataSource.setInactivityTimeout((Integer) ref.get("inactivitytimeout").getContent());
            dataSource.setJackcessOpener((String) ref.get("jackcessopener").getContent());
            dataSource.setKeepMirror((String) ref.get("keepmirror").getContent());
            dataSource.setLobScale((Integer) ref.get("lobscale").getContent());
            dataSource.setMemory((Boolean) ref.get("memory").getContent());
            dataSource.setMirrorFolder((String) ref.get("mirrorfolder").getContent());
            dataSource.setNewDatabaseVersion((String) ref.get("newdatabaseversion").getContent());
            dataSource.setOpenExclusive((Boolean) ref.get("openexclusive").getContent());
            dataSource.setPreventReloading((Boolean) ref.get("preventreloading").getContent());
            dataSource.setReMap((String) ref.get("remap").getContent());
            dataSource.setShowSchema((Boolean) ref.get("showschema").getContent());
            dataSource.setSkipIndexes((Boolean) ref.get("skipindexes").getContent());
            dataSource.setSysSchema((Boolean) ref.get("sysschema").getContent());

            return dataSource;
        } else {
            return null;
        }
    }
}
