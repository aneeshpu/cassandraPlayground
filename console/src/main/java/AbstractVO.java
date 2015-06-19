import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Result;

public abstract class AbstractVO<T extends AbstractVO<T>> {

    abstract protected T getInstance();
    abstract protected Class<T> getType();

    protected Mapper<T> getMapper(CassandraConnection.SessionWrapper sessionWrapper){
        return sessionWrapper.getMapper(getType());
    }

    public void save(CassandraConnection.SessionWrapper sessionWrapper){
        getMapper(sessionWrapper).save(getInstance());
    }

    public void delete(CassandraConnection.SessionWrapper sessionWrapper){
        getMapper(sessionWrapper).delete(getInstance());
    }

    public T get(CassandraConnection.SessionWrapper sessionWrapper, Object... primaryKeyComponents ){
        return getMapper(sessionWrapper).get(primaryKeyComponents);
    }

    public Result<T> map(CassandraConnection.SessionWrapper sessionWrapper, ResultSet resultSet){
        return getMapper(sessionWrapper).map(resultSet);
    }
}