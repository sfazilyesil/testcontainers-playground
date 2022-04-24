package org.example;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSQLIntegrationTest  {
    private static final Logger logger = LogManager.getLogger(PostgreSQLIntegrationTest.class.getName());

    private static final String IMAGE_POSTGRES = "postgres:10.10";
    private static final String INIT_SCRIPT_FILE_PATH = "postgres-init.sql";
    private static final String INIT_SCRIPT_CONTAINER_PATH = "/docker-entrypoint-initdb.d/postgres-init.sql";
    private static final Integer INIT_SCRIPT_MODE = 0777;

    private static final String DB_USER = "testUser";
    private static final String DB_PASS = "pass123" ;

    @Container
    private static final PostgreSQLContainer postgresqlContainer = new PostgreSQLContainer<>(IMAGE_POSTGRES)
            .withUsername(DB_USER)
            .withPassword(DB_PASS)
            .withCopyFileToContainer(MountableFile.forClasspathResource(INIT_SCRIPT_FILE_PATH, INIT_SCRIPT_MODE), INIT_SCRIPT_CONTAINER_PATH);

    @Test
    @DisplayName("container should be ready")
    void container_should_be_ready() {
        assertTrue(postgresqlContainer.isRunning());
    }

    @Test
    @DisplayName("container logs should contain ready message")
    void container_logs_should_contain_ready_message() {
        String containerLogs = postgresqlContainer.getLogs();
        logger.debug("Container logs: ---\n{}\n---", containerLogs);

        assertTrue(StringUtils.contains(containerLogs, "database system is ready to accept connections"));
    }

    @Test
    @DisplayName("container logs should not contain error")
    void container_logs_should_not_contain_error() {
        String containerLogs = postgresqlContainer.getLogs();
        assertFalse(StringUtils.contains(containerLogs, "ERROR"));
    }

    @Test
    @DisplayName("should contain two records in giants table")
    void should_contain_two_records_in_giants_table() throws SQLException {
        String jdbcUrl = postgresqlContainer.getJdbcUrl();
        logger.debug("Jdbc Url: {}", jdbcUrl);

        String url = String.format("%s&user=%s&password=%s&ssl=false", jdbcUrl, DB_USER, DB_PASS);
        logger.debug("Conn url: {}", url);

        Connection conn = DriverManager.getConnection(url);
        ResultSet resultSet = conn.createStatement().executeQuery ("select count(*) from GIANTS");
        resultSet.next();

        assertEquals(2, resultSet.getInt(1));
    }

}
