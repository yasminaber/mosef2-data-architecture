CREATE TABLE `cars_all_brands_tweets` (
   `index` bigint(20) DEFAULT NULL,
   `id` bigint(20) DEFAULT NULL,
   `user_location` text,
   `lang` text,
   `user_lang` double DEFAULT NULL,
   `place_type` text,
   `place_name` text,
   `place_full_name` text,
   `place_country_code` text,
   `place_country` text,
   `brand` text,
   `user_location_google_geocode_country_code` text,
   `user_location_google_geocode_country_name` text,
   `calculated_country_code` text,
   KEY `ix_cars_all_brands_tweets_index` (`index`)
 ) ENGINE=InnoDB DEFAULT CHARSET=latin1