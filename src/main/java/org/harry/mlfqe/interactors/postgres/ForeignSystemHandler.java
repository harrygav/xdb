package org.harry.mlfqe.interactors.postgres;

import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.JDBCProperties;

public interface ForeignSystemHandler {

    void createExtension(JDBCProperties jdbcProperties);

    void registerForeignSystem(Interactor interactor, JDBCProperties jdbcProperties);

    void registerForeignTable(Interactor interactor, JDBCProperties jdbcProperties, String tableName, String foreignSystemName);
}
