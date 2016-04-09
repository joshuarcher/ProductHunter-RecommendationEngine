CREATE DATABASE IF NOT EXISTS phunter_spark;

USE phunter_spark;

DROP TABLE IF EXISTS Recommendation;
DROP TABLE IF EXISTS Vote;
DROP TABLE IF EXISTS Product;
CREATE TABLE IF NOT EXISTS Product
(
  id int,
  name varchar(255),
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Vote
(
  createdAt varchar(255),
  userId int,
  prodId int,
  rating int DEFAULT 1,
  PRIMARY KEY(prodId, userId),
  FOREIGN KEY (prodId)
    REFERENCES Product(id)
);

CREATE TABLE IF NOT EXISTS Recommendation
(
  userId varchar(255),
  prodId varchar(255),
  prediction float,
  PRIMARY KEY(userId, prodId),
  FOREIGN KEY (prodId)
    REFERENCES Product(id)
);


