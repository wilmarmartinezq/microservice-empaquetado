CREATE TABLE eda_empaquetado(  
    id int NOT NULL PRIMARY KEY AUTO_INCREMENT COMMENT 'Llave primaria',
    producto_id VARCHAR(255),
    estado VARCHAR(255),
    fecha VARCHAR(255)

) COMMENT 'Tabla estado de los empaquetados';