-- PREIS
drop table PREISHISTORIE /

drop sequence PREISHISTORIE_SEQ /

create sequence PREISHISTORIE_SEQ /

CREATE TABLE PREISHISTORIE
(
    PREISHISTORIE_ID NUMBER default "bi21_onfi2"."PREISHISTORIE_SEQ"."NEXTVAL" not null
        constraint PREISHISTORIE_SEQ primary key,
    PRODUKT_ID       NUMBER
        constraint FK_PRODUKT_PREISHISTORIE_ID references PRODUKT,
    BETRAG           NUMBER,
    START_TIMESTAMP  DATE,
    END_TIMESTAMP    DATE,
    TYP              VARCHAR2(15 CHAR) CHECK ( Typ IN ('EINKAUFSPREIS', 'LISTENVERKAUFSPREIS'))
);

-- MARKE
Drop table MARKE /

Drop sequence MARKE_SEQ /

Create sequence MARKE_SEQ /

CREATE TABLE MARKE

(
    MARKE_ID     NUMBER default "bi21_onfi2"."MARKE_SEQ"."NEXTVAL" not null
        constraint MARKE_SEQ primary key,

    LIEFERANT_ID NUMBER
        constraint FK_MARKE_LIEFERANT_ID references LIEFERANT,

    BEZEICHNUNG  VARCHAR2(256)

);


ALTER TABLE PRODUKT
    ADD MARKE_ID NUMBER
        CONSTRAINT FK_MARKE_PRODUKT_ID references MARKE (MARKE_ID)
