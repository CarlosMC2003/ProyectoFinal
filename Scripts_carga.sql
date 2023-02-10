USE movies_data;
#----------------------TABLAMOVIE------------------------------

DROP PROCEDURE IF EXISTS TablaMovie;

DELIMITER $$
CREATE PROCEDURE TablaMovie()
BEGIN

DECLARE done INT DEFAULT FALSE;
DECLARE MovidMovie INT;
DECLARE Movindex INT;
DECLARE Movbudget BIGINT;
DECLARE Movhomepage VARCHAR(255);
DECLARE Movkeywords VARCHAR(255);
DECLARE MovidOrigLang INT;
DECLARE MovOrigLangName VARCHAR(2);
DECLARE Movoriginal_title VARCHAR(255);
DECLARE Movoverview TEXT;
DECLARE Movpopularity DOUBLE;
DECLARE Movrelease_date DATE;
DECLARE Movrevenue BIGINT;
DECLARE Movruntime DOUBLE;
DECLARE Movstatus INT;
DECLARE MovStatusName varchar(100);
DECLARE Movtagline VARCHAR(255);
DECLARE Movtitle VARCHAR(255);
DECLARE Movvote_average DOUBLE;
DECLARE Movvote_count INT;

 -- Declarar el cursor
DECLARE CursorMovie CURSOR FOR
    SELECT id,`index`,budget,homepage, keywords, original_language, original_title,overview,popularity,release_date,revenue,
               runtime, status,tagline,title, vote_count,vote_average FROM movie_dataset;

 -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

 -- Abrir el cursor
OPEN CursorMovie;
CursorMovie_loop: LOOP
    FETCH CursorMovie INTO MovidMovie, Movindex,Movbudget,Movhomepage,Movkeywords,MovOrigLangName, Movoriginal_title,Movoverview,
    Movpopularity,Movrelease_date, Movrevenue, Movruntime, MovStatusName, Movtagline,Movtitle,Movvote_average,Movvote_count;

    -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
    IF done THEN
        LEAVE CursorMovie_loop;
    END IF;

    SELECT idStatus INTO Movstatus FROM ddlfinal.status WHERE status.nameStatus = MovStatusName;

    SELECT idOringLang INTO MovidOrigLang FROM ddlfinal.original_language
    WHERE ddlfinal.original_language.name_original_language = MovOrigLangName;

    INSERT INTO ddlfinal.Movie (idMovie, `index`, budget, homepage, keywords, idOrigLang, original_title, overview, popularity,release_date,revenue,runtime, idStatus,
		tagline, title, vote_average, vote_count)
    VALUES (MovidMovie, Movindex,Movbudget,Movhomepage,Movkeywords,MovidOrigLang, Movoriginal_title,Movoverview,
    Movpopularity,Movrelease_date,Movrevenue,Movruntime, Movstatus, Movtagline,Movtitle,Movvote_average,Movvote_count);

END LOOP;
CLOSE CursorMovie;
END $$
DELIMITER $$;

CALL TablaMovie ();

SELECT COUNT(*) FROM ddlfinal.Movie;
SELECT * FROM ddlfinal.Movie;

#----------------------TABLAORIGINAL-LANGUAGE------------------------------

DROP PROCEDURE IF EXISTS TablaOriginalLanguage;

DELIMITER $$
CREATE PROCEDURE TablaOriginalLanguage()
BEGIN

DECLARE done INT DEFAULT FALSE;
DECLARE nameOL VARCHAR(2);

 -- Declarar el cursor
DECLARE CursorOL CURSOR FOR
    SELECT DISTINCT original_language AS names from movie_dataset;

 -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

 -- Abrir el cursor
OPEN CursorOL;
CursorOL_loop: LOOP
    FETCH CursorOL INTO nameOL;

-- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
    IF done THEN
        LEAVE CursorOL_loop;
    END IF;
    IF nameOL IS NULL THEN
        SET nameOL = '';
    END IF;
    SET @_oStatement = CONCAT('INSERT INTO ddlFinal.original_language (name_original_language) VALUES (\'',
	nameOL,'\');');
    PREPARE sent1 FROM @_oStatement;
    EXECUTE sent1;
    DEALLOCATE PREPARE sent1;

END LOOP;
CLOSE CursorOL;
END $$
DELIMITER ;

CALL TablaOriginalLanguage();

SELECT * FROM ddlfinal.original_language;

#----------------------TABLASTATUS------------------------------

DROP PROCEDURE IF EXISTS TablaStatus;

DELIMITER $$
CREATE PROCEDURE TablaStatus()
BEGIN

DECLARE done INT DEFAULT FALSE;
DECLARE nameStatus VARCHAR(15);

 -- Declarar el cursor
DECLARE CursorStatus CURSOR FOR
    SELECT DISTINCT CONVERT(status USING UTF8MB4) AS names from movie_dataset;

 -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

 -- Abrir el cursor
OPEN CursorStatus;
CursorStatus_loop: LOOP
    FETCH CursorStatus INTO nameStatus;

-- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
    IF done THEN
        LEAVE CursorStatus_loop;
    END IF;
    IF nameStatus IS NULL THEN
        SET nameStatus = '';
    END IF;
    SET @_oStatement = CONCAT('INSERT INTO ddlFinal.Status (nameStatus) VALUES (\'',
	nameStatus,'\');');
    PREPARE sent1 FROM @_oStatement;
    EXECUTE sent1;
    DEALLOCATE PREPARE sent1;

END LOOP;
CLOSE CursorStatus;
END $$
DELIMITER ;

CALL TablaStatus();

SELECT * FROM ddlFinal.Status;

#----------------------TABLAPRODUCTION_COMPANIES------------------------------

DROP PROCEDURE IF EXISTS TablaProduction_companies;

DELIMITER $$
CREATE PROCEDURE TablaProduction_companies ()

BEGIN

 DECLARE done INT DEFAULT FALSE ;
 DECLARE jsonData json ;
 DECLARE jsonId varchar(250) ;
 DECLARE jsonLabel varchar(250) ;
 DECLARE resultSTR LONGTEXT DEFAULT '';
 DECLARE i INT;

 -- Declarar el cursor
 DECLARE myCursor
  CURSOR FOR
   SELECT JSON_EXTRACT(CONVERT(production_companies USING UTF8MB4), '$[*]') FROM movie_dataset ;

 -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
 DECLARE CONTINUE HANDLER
  FOR NOT FOUND SET done = TRUE ;

 -- Abrir el cursor
 OPEN myCursor  ;
 drop table if exists production_companietem;
    SET @sql_text = 'CREATE TABLE production_companieTem ( id int, nameCom VARCHAR(100));';
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
 cursorLoop: LOOP
  FETCH myCursor INTO jsonData;

  -- Controlador para buscar cada uno de los arrays
    SET i = 0;

  -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
  IF done THEN
   LEAVE  cursorLoop ;
  END IF ;

  WHILE(JSON_EXTRACT(jsonData, CONCAT('$[', i, ']')) IS NOT NULL) DO
  SET jsonId = IFNULL(JSON_EXTRACT(jsonData,  CONCAT('$[', i, '].id')), '') ;
  SET jsonLabel = IFNULL(JSON_EXTRACT(jsonData, CONCAT('$[', i,'].name')), '') ;
  SET i = i + 1;

  SET @sql_text = CONCAT('INSERT INTO production_companieTem VALUES (', REPLACE(jsonId,'\'',''), ', ', jsonLabel, '); ');
	PREPARE stmt FROM @sql_text;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

  END WHILE;

 END LOOP ;

 select distinct * from production_companieTem;
    INSERT INTO ddlfinal.production_companies
    SELECT DISTINCT id, nameCom
    FROM production_companieTem;
    drop table if exists production_companieTem;
 CLOSE myCursor ;

END$$
DELIMITER ;

call TablaProduction_companies();

CREATE TABLE production_companiesCURSOR (
	id INT PRIMARY KEY,
    name varchar(100)
);

DROP TABLE production_companiesCURSOR;

SELECT * FROM ddlfinal.production_companies;
SELECT COUNT(*) FROM ddlfinal.production_companies;

#----------------------TABLAPRODUCTION_COUNTRIES------------------------------

DROP PROCEDURE IF EXISTS TablaProduction_countries;

DELIMITER $$
CREATE PROCEDURE TablaProduction_countries ()

BEGIN

 DECLARE done INT DEFAULT FALSE ;
 DECLARE jsonData json ;
 DECLARE jsonId varchar(250) ;
 DECLARE jsonLabel varchar(250) ;
 DECLARE resultSTR LONGTEXT DEFAULT '';
 DECLARE i INT;

 -- Declarar el cursor
 DECLARE myCursor
  CURSOR FOR
   SELECT JSON_EXTRACT(CONVERT(production_countries USING UTF8MB4), '$[*]') FROM movie_dataset ;

 -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
 DECLARE CONTINUE HANDLER
  FOR NOT FOUND SET done = TRUE ;

 -- Abrir el cursor
 OPEN myCursor  ;
 drop table if exists production_countriesTem;
    SET @sql_text = 'CREATE TABLE production_countriesTem ( iso_3166_1 varchar(2), nameCountry VARCHAR(100));';
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
 cursorLoop: LOOP
  FETCH myCursor INTO jsonData;

  -- Controlador para buscar cada uno de los arrays
    SET i = 0;

  -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
  IF done THEN
   LEAVE  cursorLoop ;
  END IF ;

  WHILE(JSON_EXTRACT(jsonData, CONCAT('$[', i, ']')) IS NOT NULL) DO
  SET jsonId = IFNULL(JSON_EXTRACT(jsonData,  CONCAT('$[', i, '].iso_3166_1')), '') ;
  SET jsonLabel = IFNULL(JSON_EXTRACT(jsonData, CONCAT('$[', i,'].name')), '') ;
  SET i = i + 1;

  SET @sql_text = CONCAT('INSERT INTO production_countriesTem VALUES (', REPLACE(jsonId,'\'',''), ', ', jsonLabel, '); ');
	PREPARE stmt FROM @sql_text;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

  END WHILE;

 END LOOP ;

 select distinct * from production_countriesTem;
    INSERT INTO ddlfinal.production_countries
    SELECT DISTINCT iso_3166_1, nameCountry
    FROM production_countriesTem;
    drop table if exists production_countriesTem;
 CLOSE myCursor ;

END$$
DELIMITER ;

call TablaProduction_countries();

SELECT COUNT(*) FROM ddlfinal.production_countries;

#----------------------TABLASPOKEN_LANGUAGES------------------------------

DROP PROCEDURE IF EXISTS TablaSpokenLanguages;

DELIMITER $$
CREATE PROCEDURE TablaSpokenLanguages ()

BEGIN

 DECLARE done INT DEFAULT FALSE ;
 DECLARE jsonData json ;
 DECLARE jsonId varchar(250) ;
 DECLARE jsonLabel varchar(250) ;
 DECLARE resultSTR LONGTEXT DEFAULT '';
 DECLARE i INT;

 -- Declarar el cursor
 DECLARE myCursor
  CURSOR FOR
   SELECT JSON_EXTRACT(CONVERT(spoken_languages USING UTF8MB4), '$[*]') FROM movie_dataset ;

 -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
 DECLARE CONTINUE HANDLER
  FOR NOT FOUND SET done = TRUE ;

 -- Abrir el cursor
 OPEN myCursor  ;
 drop table if exists spokenLanguagesTem;
    SET @sql_text = 'CREATE TABLE spokenLanguagesTem ( iso_639_1 varchar(2), nameLang VARCHAR(100));';
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
 cursorLoop: LOOP
  FETCH myCursor INTO jsonData;

  -- Controlador para buscar cada uno de los arrays
    SET i = 0;

  -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
  IF done THEN
   LEAVE  cursorLoop ;
  END IF ;

  WHILE(JSON_EXTRACT(jsonData, CONCAT('$[', i, ']')) IS NOT NULL) DO
  SET jsonId = IFNULL(JSON_EXTRACT(jsonData,  CONCAT('$[', i, '].iso_639_1')), '') ;
  SET jsonLabel = IFNULL(JSON_EXTRACT(jsonData, CONCAT('$[', i,'].name')), '') ;
  SET i = i + 1;

  SET @sql_text = CONCAT('INSERT INTO spokenLanguagesTem VALUES (', REPLACE(jsonId,'\'',''), ', ', jsonLabel, '); ');
	PREPARE stmt FROM @sql_text;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

  END WHILE;

 END LOOP ;

 select distinct * from spokenLanguagesTem;
    INSERT INTO ddlfinal.spoken_language
    SELECT DISTINCT iso_639_1, nameLang
    FROM spokenLanguagesTem;
    drop table if exists spokenLanguagesTem;
 CLOSE myCursor ;

END$$
DELIMITER ;

call TablaSpokenLanguages();

SELECT * FROM ddlfinal.spoken_language;

SELECT COUNT(*) FROM ddlfinal.spoken_language;

#----------------------TABLAMOVIE_PRODUCTION_COMPANIES------------------------------

DROP PROCEDURE IF EXISTS TablaMovie_production_companies;

DELIMITER $$
CREATE PROCEDURE TablaMovie_production_companies ()

BEGIN

 DECLARE done INT DEFAULT FALSE;
 DECLARE idMovie int;
 DECLARE idProdComp JSON;
 DECLARE idJSON text;
 DECLARE i INT;

 -- Declarar el cursor
 DECLARE myCursor
  CURSOR FOR
   SELECT id, production_companies FROM movie_dataset;

 -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
 DECLARE CONTINUE HANDLER
  FOR NOT FOUND SET done = TRUE ;

 -- Abrir el cursor
 OPEN myCursor  ;

 drop table if exists MovieProdCompTemp;
    SET @sql_text = 'CREATE TABLE MovieProdCompTemp ( id int, idGenre int );';
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

 cursorLoop: LOOP

     FETCH myCursor INTO idMovie, idProdComp;

  -- Controlador para buscar cada uno de los arrays
    SET i = 0;

  -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
  IF done THEN
   LEAVE  cursorLoop ;
  END IF ;

  WHILE(JSON_EXTRACT(idProdComp, CONCAT('$[', i, '].id')) IS NOT NULL) DO

  SET idJSON = JSON_EXTRACT(idProdComp,  CONCAT('$[', i, '].id')) ;
  SET i = i + 1;

  SET @sql_text = CONCAT('INSERT INTO MovieProdCompTemp VALUES (', idMovie, ', ', REPLACE(idJSON,'\'',''), '); ');
	PREPARE stmt FROM @sql_text;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

  END WHILE;

 END LOOP ;

 select distinct * from MovieProdCompTemp;
    INSERT INTO ddlfinal.Movie_production_companies
    SELECT DISTINCT id, idGenre
    FROM MovieProdCompTemp;
    drop table if exists MovieProdCompTemp;
 CLOSE myCursor ;

END$$
DELIMITER ;

call TablaMovie_production_companies();

#----------------------TABLAMOVIE_SPOKEN_LANGUAGES------------------------------

DROP PROCEDURE IF EXISTS TablaMovie_spoken_languages;

DELIMITER $$
CREATE PROCEDURE TablaMovie_spoken_languages ()

BEGIN

 DECLARE done INT DEFAULT FALSE;
 DECLARE idMovie int;
 DECLARE idSpokLang text;
 DECLARE idJSON text;
 DECLARE i INT;

 -- Declarar el cursor
 DECLARE myCursor
  CURSOR FOR
   SELECT id, spoken_languages FROM movie_dataset;

 -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
 DECLARE CONTINUE HANDLER
  FOR NOT FOUND SET done = TRUE ;

 -- Abrir el cursor
 OPEN myCursor  ;

 cursorLoop: LOOP

     FETCH myCursor INTO idMovie, idSpokLang;

  -- Controlador para buscar cada uno de los arrays
    SET i = 0;

  -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
  IF done THEN
   LEAVE  cursorLoop ;
  END IF ;

  WHILE(JSON_EXTRACT(idSpokLang, CONCAT('$[', i, '].iso_639_1')) IS NOT NULL) DO

  SET idJSON = JSON_EXTRACT(idSpokLang,  CONCAT('$[', i, '].iso_639_1')) ;
  SET i = i + 1;

  SET @sql_text = CONCAT('INSERT INTO ddlFinal.Movie_spoken_languages VALUES (', idMovie, ', ', REPLACE(idJSON,'\'',''), '); ');
	PREPARE stmt FROM @sql_text;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

  END WHILE;

 END LOOP ;
 CLOSE myCursor ;

END$$
DELIMITER ;

call TablaMovie_spoken_languages();

#----------------------TABLAMOVIE_PRODUCTION_COUNTRIES------------------------------

DROP PROCEDURE IF EXISTS TablaMovie_production_countries;

DELIMITER $$
CREATE PROCEDURE TablaMovie_production_countries ()

BEGIN

 DECLARE done INT DEFAULT FALSE;
 DECLARE idMovie int;
 DECLARE idProdCoun text;
 DECLARE idJSON text;
 DECLARE i INT;

 -- Declarar el cursor
 DECLARE myCursor
  CURSOR FOR
   SELECT id, production_countries FROM movie_dataset;

 -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
 DECLARE CONTINUE HANDLER
  FOR NOT FOUND SET done = TRUE ;

 -- Abrir el cursor
 OPEN myCursor  ;

 drop table if exists MovieProdCompTemp;

    SET @sql_text = 'CREATE TABLE MovieProdCompTemp ( id int, idGenre varchar(255) );';
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

 cursorLoop: LOOP

     FETCH myCursor INTO idMovie, idProdCoun;

  -- Controlador para buscar cada uno de los arrays
    SET i = 0;

  -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
  IF done THEN
   LEAVE  cursorLoop ;
  END IF ;

  WHILE(JSON_EXTRACT(idProdCoun, CONCAT('$[', i, '].iso_3166_1')) IS NOT NULL) DO

  SET idJSON = JSON_EXTRACT(idProdCoun,  CONCAT('$[', i, '].iso_3166_1')) ;
  SET i = i + 1;

  SET @sql_text = CONCAT('INSERT INTO MovieProdCompTemp VALUES (', idMovie, ', ', REPLACE(idJSON,'\'',''), '); ');
	PREPARE stmt FROM @sql_text;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

  END WHILE;

 END LOOP ;

 select distinct * from MovieProdCompTemp;
    INSERT INTO ddlfinal.Movie_production_countries
    SELECT DISTINCT id, idGenre
    FROM MovieProdCompTemp;
    drop table if exists MovieProdCompTemp;
 CLOSE myCursor ;

END$$
DELIMITER ;

call TablaMovie_production_countries();