CREATE DATABASE test_db;
USE test_db;

CREATE TABLE test_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    value INT NOT NULL
);

INSERT INTO test_table (name, value) VALUES ('Alice', 10), ('Bob', 20), ('Charlie', 30);
