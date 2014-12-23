-- --------------------------------------------------------------------------- --
-- Creating database
--

CREATE DATABASE `isidaplus` CHARACTER SET utf8 COLLATE utf8_general_ci;
CREATE USER `isidaplus`@`localhost` IDENTIFIED BY 'isidaplus';
GRANT ALL PRIVILEGES ON `isidaplus`.* TO `isidaplus`@`localhost`;

FLUSH PRIVILEGES;
USE `isidaplus`


-- --------------------------------------------------------------------------- --
-- Plugins list for plugin manager
--
CREATE TABLE plugins (
    directory VARCHAR(255) PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    description TEXT,
    url VARCHAR(255) DEFAULT 'Native',
    vcs VARCHAR(10) DEFAULT 'git',
    supcommands TEXT NOT NULL,
    version VARCHAR(20),
    autoload TINYINT DEFAULT 0,
    loaded TINYINT DEFAULT 0
);

CREATE UNIQUE INDEX plugin_names ON plugins (name);