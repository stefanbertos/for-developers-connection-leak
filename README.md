# for-developers-connection-leak

https://www.kaggle.com/datasets/jainilcoder/online-payment-fraud-detection

CREATE TABLE payment (
step           NUMBER NOT NULL,
type           VARCHAR2(50),
amount         NUMBER,
nameorig       VARCHAR2(50),
oldbalanceorg  NUMBER,
newbalanceorig NUMBER,
namedest       VARCHAR2(50),
oldbalancedest NUMBER,
newbalancedest NUMBER,
isfraud        NUMBER,
isflaggedfraud NUMBER
);