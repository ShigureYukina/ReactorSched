package com.yuki.reactorsched.faultlab;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Injects database faults into every write statement (PRD 8.8): {@code READ_ONLY} fails the write,
 * {@code LATENCY} delays it. Reads stay healthy, so claim queries still run while the conditional
 * updates that claim a task fail - exactly the inconsistency the recovery tests exercise.
 *
 * <p>Audit statements are always exempt: the control endpoints themselves write audit rows, and a
 * fault that could not be switched off would be a dead lock, not a fault.
 */
@Profile("!executor")
@Component
@Intercepts(@Signature(type = Executor.class, method = "update",
        args = {MappedStatement.class, Object.class}))
public class DatabaseFaultInterceptor implements Interceptor {

    private static final Logger log = LoggerFactory.getLogger(DatabaseFaultInterceptor.class);

    private final DatabaseFaultState state;

    public DatabaseFaultInterceptor(DatabaseFaultState state) {
        this.state = state;
    }

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        MappedStatement statement = (MappedStatement) invocation.getArgs()[0];
        if (statement.getId().contains("OperationAuditMapper")) {
            return invocation.proceed();
        }
        DatabaseFaultMode mode = state.mode();
        if (mode == DatabaseFaultMode.READ_ONLY) {
            log.warn("fault injection: write {} refused, the database is READ_ONLY", statement.getId());
            throw new PersistenceException(new SQLException(
                    "fault injection: the database is READ_ONLY (PRD 8.8)"));
        }
        if (mode == DatabaseFaultMode.LATENCY && state.latencyMs() > 0) {
            Thread.sleep(state.latencyMs());
        }
        return invocation.proceed();
    }

    @Override
    public Object plugin(Object target) {
        return target instanceof Executor ? Plugin.wrap(target, this) : target;
    }

    @Override
    public void setProperties(Properties properties) {
        // no properties
    }
}
