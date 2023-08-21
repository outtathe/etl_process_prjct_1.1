CREATE OR REPLACE FUNCTION ds.get_credit_debit_info(target_date DATE)
RETURNS TABLE (
    oper_date DATE,
    max_credit_amount FLOAT,
    min_credit_amount FLOAT,
    max_debit_amount FLOAT,
    min_debit_amount FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        ds.ft_posting_f.oper_date,
        MAX(credit_amount) AS max_credit_amount,
        MIN(credit_amount) AS min_credit_amount,
        MAX(debet_amount) AS max_debit_amount,
        MIN(debet_amount) AS min_debit_amount
    FROM DS.FT_POSTING_F
    WHERE ds.ft_posting_f.oper_date = target_date
    GROUP BY ds.ft_posting_f.oper_date;
END;
$$ LANGUAGE plpgsql;
