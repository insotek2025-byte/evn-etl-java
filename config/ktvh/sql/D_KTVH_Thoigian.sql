DECLARE
    v_start_date DATE := DATE '2000-01-01';
    v_end_date   DATE := DATE '2040-12-31';
    v_date       DATE;
BEGIN
    v_date := v_start_date;
    WHILE v_date <= v_end_date LOOP
        INSERT INTO D_KTVH_Thoigian (
            ID_ThoiGian, NGAY, THANG, NAM, FULL_DATE
        )
        VALUES (
            TO_NUMBER(TO_CHAR(v_date, 'YYYYMMDD')), -- ID = 20250824
            TO_NUMBER(TO_CHAR(v_date, 'DD')),
            TO_NUMBER(TO_CHAR(v_date, 'MM')),
            TO_NUMBER(TO_CHAR(v_date, 'YYYY')),
            v_date
        );
        v_date := v_date + 1;
    END LOOP;
    COMMIT;
END;
