CREATE TABLE ordenes (  
    id int NOT NULL PRIMARY KEY AUTO_INCREMENT COMMENT 'Llave primaria',
    nombre VARCHAR(255),
    producto VARCHAR(255),
    cantidad INT,
    email VARCHAR(255),
    direccion VARCHAR(255),
    fecha_creacion VARCHAR(255)

) COMMENT 'Tabla estado de los empaquetados';