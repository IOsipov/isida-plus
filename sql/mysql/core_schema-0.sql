-- --------------------------------------------------------------------------- --
-- Plugins list for pmanager
--
CREATE TABLE plugins (
    file text,
    name text,
    description text,
    supcommands text,
    version text,
    autoload integer default 0,
    loaded integer
);

CREATE UNIQUE INDEX plugin_files ON plugins (file(255));
CREATE UNIQUE INDEX plugin_names ON plugins (name(255));