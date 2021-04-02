CREATE TABLE `github_languages` (
   `index` bigint(20) DEFAULT NULL,
   `name` text,
   `native` text,
   `language_code` text,
   `rtl` double DEFAULT NULL,
   KEY `ix_github_languages_index` (`index`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1