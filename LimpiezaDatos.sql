USE movies_data;

DROP TABLE IF EXISTS NewCrewTemp;

#----------------------LIMPIEZA-CREW------------------------------

CREATE TABLE NewCrewTemp(
    SELECT id,
CONVERT (
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(crew,
'"', '\''),
'{\'', '{"'),
'\': \'', '": "'),
'\', \'', '", "'),
'\': ', '": '),
', \'', ', "')
USING UTF8mb4 ) AS crew_new
FROM movie_dataset
)

#----------------------LIMPIEZA-GENRES------------------------------

DROP PROCEDURE IF EXISTS TablaGenre;

DELIMITER $$
CREATE PROCEDURE TablaGenre()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE nameGenre VARCHAR(100);

    -- Declarar el cursor
    DECLARE Cursorgenre CURSOR FOR
        SELECT DISTINCT CONVERT(
            REPLACE(
            REPLACE(genres,
                'Science Fiction', 'Science_Fiction'),
                'TV Movie', 'TV_Movie')
            USING UTF8MB4)
        FROM movie_dataset;

    -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Abrir el cursor
    OPEN Cursorgenre;
    drop table if exists temperolgenre;
    SET @sql_text = 'CREATE TABLE temperolgenre (name VARCHAR(100));';
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    CursorDirector_loop: LOOP
        FETCH Cursorgenre INTO nameGenre;

        -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
        IF done THEN
            LEAVE CursorDirector_loop;
        END IF;

        -- Separar los géneros en una tabla temporal
        DROP TEMPORARY TABLE IF EXISTS temp_genres;
        CREATE TEMPORARY TABLE temp_genres (genre VARCHAR(50));
        SET @_genres = nameGenre;
        WHILE (LENGTH(@_genres) > 0) DO
                SET @_genre = TRIM(SUBSTRING_INDEX(@_genres, ' ', 1));
                INSERT INTO temp_genres (genre) VALUES (@_genre);
                SET @_genres = SUBSTRING(@_genres, LENGTH(@_genre) + 2);
            END WHILE;

        -- Insertar los géneros separados en filas individuales
        INSERT INTO temperolgenre (name)
        SELECT genre FROM temp_genres;
    END LOOP CursorDirector_loop;
    select distinct * from temperolgenre;
    INSERT INTO ddlfinal.genre (nameGenre)
    SELECT DISTINCT nameGenre
    FROM temperolgenre;
    drop table if exists temperolgenre;
    CLOSE Cursorgenre;
END $$
DELIMITER ;

CALL TablaGenre();