package company.vk.edu.distrib.compute.bobridze5.handlers;

import company.vk.edu.distrib.compute.Dao;

public abstract class DaoHandler<T> extends AbstractHandler {
    protected final Dao<T> dao;

    public DaoHandler(Dao<T> dao) {
        super();
        this.dao = dao;
    }
}
