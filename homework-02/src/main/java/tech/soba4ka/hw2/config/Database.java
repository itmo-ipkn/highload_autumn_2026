package tech.soba4ka.hw2.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Database {
    public static Connection open() throws SQLException {
        return DriverManager.getConnection(System.getenv("DB_URL"));
    }
}
