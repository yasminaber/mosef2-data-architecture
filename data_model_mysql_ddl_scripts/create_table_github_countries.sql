CREATE TABLE `github_countries` (
   `index` bigint(20) DEFAULT NULL,
   `name` text,
   `native` text,
   `phone` text,
   `continent` text,
   `capital` text,
   `currency` text,
   `languages` text,
   `country_code` text,
   KEY `ix_github_countries_index` (`index`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1