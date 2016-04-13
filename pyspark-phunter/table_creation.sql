CREATE DATABASE IF NOT EXISTS phunter_spark;

USE phunter_spark;

DROP TABLE IF EXISTS Recommendation;
DROP TABLE IF EXISTS Vote;
DROP TABLE IF EXISTS Product;
CREATE TABLE IF NOT EXISTS Product
(
  id int NOT NULL PRIMARY KEY,
  userId int NOT NULL
)ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS Vote
(
  userId int NOT NULL,
  prodId int NOT NULL,
  rating int,
  PRIMARY KEY(prodId, userId),
  CONSTRAINT FOREIGN KEY (prodId) REFERENCES Product(id)
)ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS Recommendation
(
  userId int,
  prodId int,
  prediction float,
  PRIMARY KEY(userId, prodId),
  CONSTRAINT FOREIGN KEY (prodId) REFERENCES Product(id)
)ENGINE=InnoDB;
