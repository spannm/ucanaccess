package net.ucanaccess.jdbc;

import net.ucanaccess.converters.Metadata.Property;

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
            dataSource.setUser((String) ref.get(Property.user.name()).getContent());
            dataSource.setPassword((String) ref.get(Property.password.name()).getContent());

            dataSource.setColumnOrder((String) ref.get(Property.columnOrder.name()).getContent());
            dataSource.setConcatNulls((Boolean) ref.get(Property.concatNulls.name()).getContent());
            dataSource.setEncrypt((Boolean) ref.get(Property.encrypt.name()).getContent());
            dataSource.setIgnoreCase((Boolean) ref.get(Property.ignoreCase.name()).getContent());
            dataSource.setImmediatelyReleaseResources((Boolean) ref.get(Property.immediatelyReleaseResources.name()).getContent());
            dataSource.setInactivityTimeout((Integer) ref.get(Property.inactivityTimeout.name()).getContent());
            dataSource.setJackcessOpener((String) ref.get(Property.jackcessOpener.name()).getContent());
            dataSource.setKeepMirror((String) ref.get(Property.keepMirror.name()).getContent());
            dataSource.setLobScale((Integer) ref.get(Property.lobScale.name()).getContent());
            dataSource.setMemory((Boolean) ref.get(Property.memory.name()).getContent());
            dataSource.setMirrorFolder((String) ref.get(Property.mirrorFolder.name()).getContent());
            dataSource.setNewDatabaseVersion((String) ref.get(Property.newDatabaseVersion.name()).getContent());
            dataSource.setOpenExclusive((Boolean) ref.get(Property.openExclusive.name()).getContent());
            dataSource.setPreventReloading((Boolean) ref.get(Property.preventReloading.name()).getContent());
            dataSource.setReMap((String) ref.get(Property.reMap.name()).getContent());
            dataSource.setShowSchema((Boolean) ref.get(Property.showSchema.name()).getContent());
            dataSource.setSkipIndexes((Boolean) ref.get(Property.skipIndexes.name()).getContent());
            dataSource.setSysSchema((Boolean) ref.get(Property.sysSchema.name()).getContent());

            return dataSource;
        } else {
            return null;
        }
    }
}
