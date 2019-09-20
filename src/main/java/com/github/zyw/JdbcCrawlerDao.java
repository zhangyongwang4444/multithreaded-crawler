package com.github.zyw;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JdbcCrawlerDao implements CrawlerDao {
    private static final String USER_NAME = "root";
    private static final String PASSWORD = "root";


    private final Connection connection;

    @SuppressFBWarnings("DMI_CONSTANT_DB_PASSWORD")
    public JdbcCrawlerDao() {
        try {
            this.connection = DriverManager.getConnection("jdbc:h2:file:\\C:\\Users\\34936\\IdeaProjects\\multithreaded-crawler\\news", USER_NAME, PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }


    public String getNextLink(String sql) throws SQLException {

        try (PreparedStatement statement = connection.prepareStatement(sql); ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                return resultSet.getString(1);
            }
        }
        return null;
    }

    public String getNextLinkThenDelete() throws SQLException {
        String link = getNextLink("select link from \n" + "LINKS_TO_BE_PROCESSED LIMIT 1");
        if (link != null) {
            updateDatabase(link, "DELETE FROM LINKS_TO_BE_PROCESSED where link = ?");
        }
        return link;
    }

    public void updateDatabase(String link, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, link);
            statement.executeUpdate();
        }
    }

    public void insertNewsIntoDatabase(String url, String title, String content) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("insert into NEWS (URL, TITLE, CONTENT, CREATED_AT, MODIFIED_AT) VALUES (?,?,?,now(),now() )")) {
            statement.setString(1, url);
            statement.setString(2, title);
            statement.setString(3, content);
            statement.executeUpdate();
        }
    }

    public boolean isLinkProcessed(String link) throws SQLException {
        ResultSet resultSet = null;
        try (PreparedStatement statement = connection.prepareStatement("select link from LINKS_ALREADY_PROCESSED where link = ?")) {
            statement.setString(1, link);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return true;
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return false;
    }
}
