CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f ( 
  i_OnDate  DATE
)
LANGUAGE plpgsql    
AS $$
DECLARE
	v_RowCount INT;
BEGIN
    CALL dm.writelog('[BEGIN] fill(i_OnDate => date ''' 
         || TO_CHAR(i_OnDate, 'yyyy-mm-dd') 
         || ''');', 1
       );
    
    CALL dm.writelog('delete on_date = ' 
         || TO_CHAR(i_OnDate, 'yyyy-mm-dd'), 1
       );

    DELETE
      FROM dm.DM_F101_ROUND_F f
     WHERE from_date = DATE_TRUNC('month', i_OnDate)  
       AND to_date = (DATE_TRUNC('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day');
   
    CALL dm.writelog('insert', 1);
   
    INSERT INTO dm.dm_f101_round_f
           ( from_date         
           , to_date           
           , chapter           
           , ledger_account    
           , characteristic    
           , balance_in_rub    
           , balance_in_val    
           , balance_in_total  
           , turn_deb_rub      
           , turn_deb_val      
           , turn_deb_total    
           , turn_cre_rub      
           , turn_cre_val      
           , turn_cre_total    
           , balance_out_rub  
           , balance_out_val   
           , balance_out_total 
           )
    SELECT  DATE_TRUNC('month', i_OnDate) AS from_date,
           (DATE_TRUNC('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day') AS to_date,
           s.chapter AS chapter,
           SUBSTR(acc_d.account_number, 1, 5) AS ledger_account,
           acc_d.char_type AS characteristic,
           -- RUB balance
           SUM( CASE 
                  WHEN cur.currency_code IN ('643', '810')
                  THEN b.balance_out
                  ELSE 0
                 END
              ) AS balance_in_rub,
          -- VAL balance converted to rub
          SUM( CASE 
                 WHEN cur.currency_code NOT IN ('643', '810')
                 THEN b.balance_out * exch_r.reduced_cource
                 ELSE 0
                END
             ) AS balance_in_val,
          -- Total: RUB balance + VAL converted to rub
          SUM(  CASE 
                 WHEN cur.currency_code IN ('643', '810')
                 THEN b.balance_out
                 ELSE b.balance_out * exch_r.reduced_cource
               END
             ) AS balance_in_total,
           -- RUB debet turnover
           SUM(CASE 
                 WHEN cur.currency_code IN ('643', '810')
                 THEN at.debet_amount_rub
                 ELSE 0
               END
           ) AS turn_deb_rub,
           -- VAL debet turnover converted
           SUM(CASE 
                 WHEN cur.currency_code NOT IN ('643', '810')
                 THEN at.debet_amount_rub
                 ELSE 0
               END
           ) AS turn_deb_val,
           -- SUM = RUB debet turnover + VAL debet turnover converted
           SUM(at.debet_amount_rub) AS turn_deb_total,
           -- RUB credit turnover
           SUM(CASE 
                 WHEN cur.currency_code IN ('643', '810')
                 THEN at.credit_amount_rub
                 ELSE 0
               END
              ) AS turn_cre_rub,
           -- VAL credit turnover converted
           SUM(CASE 
                 WHEN cur.currency_code NOT IN ('643', '810')
                 THEN at.credit_amount_rub
                 ELSE 0
               END
              ) AS turn_cre_val,
           -- SUM = RUB credit turnover + VAL credit turnover converted
           SUM(at.credit_amount_rub) AS turn_cre_total,
           -- Calculate BALANCE_OUT_RUB
           CASE
               WHEN acc_d.char_type = 'A' AND cur.currency_code IN ('643', '810') THEN
                   SUM( CASE 
                  WHEN cur.currency_code IN ('643', '810')
                  THEN b.balance_out
                  ELSE 0
                 END
              ) - SUM(CASE 
                 WHEN cur.currency_code IN ('643', '810')
                 THEN at.credit_amount_rub
                 ELSE 0
               END
              )  + SUM(CASE 
                 WHEN cur.currency_code IN ('643', '810')
                 THEN at.debet_amount_rub
                 ELSE 0
               END
           ) 
               WHEN acc_d.char_type = 'P' AND cur.currency_code IN ('643', '810') THEN
                   SUM( CASE 
                  WHEN cur.currency_code IN ('643', '810')
                  THEN b.balance_out
                  ELSE 0
                 END
              )  + SUM(CASE 
                 WHEN cur.currency_code IN ('643', '810')
                 THEN at.credit_amount_rub
                 ELSE 0
               END
              )  - SUM(CASE 
                 WHEN cur.currency_code IN ('643', '810')
                 THEN at.debet_amount_rub
                 ELSE 0
               END
           ) 
               ELSE NULL
           END AS balance_out_rub,
           -- Calculate BALANCE_OUT_VAL
           CASE
               WHEN acc_d.char_type = 'A' AND cur.currency_code NOT IN ('643', '810') THEN
                    SUM( CASE 
                 WHEN cur.currency_code NOT IN ('643', '810')
                 THEN b.balance_out * exch_r.reduced_cource
                 ELSE 0
                END
             ) - SUM(CASE 
                 WHEN cur.currency_code NOT IN ('643', '810')
                 THEN at.credit_amount_rub
                 ELSE 0
               END
              ) + SUM(CASE 
                 WHEN cur.currency_code NOT IN ('643', '810')
                 THEN at.debet_amount_rub
                 ELSE 0
               END
           )
               WHEN acc_d.char_type = 'P' AND cur.currency_code NOT IN ('643', '810') THEN
                   SUM( CASE 
                 WHEN cur.currency_code NOT IN ('643', '810')
                 THEN b.balance_out * exch_r.reduced_cource
                 ELSE 0
                END
             ) + SUM(CASE 
                 WHEN cur.currency_code NOT IN ('643', '810')
                 THEN at.credit_amount_rub
                 ELSE 0
               END
              ) - SUM(CASE 
                 WHEN cur.currency_code NOT IN ('643', '810')
                 THEN at.debet_amount_rub
                 ELSE 0
               END
           )
               ELSE NULL
           END AS balance_out_val,
           -- Calculate BALANCE_OUT_TOTAl
		   cast(null as numeric)                  as balance_out_total
      FROM ds.md_ledger_account_s s
      JOIN ds.md_account_d acc_d
        ON SUBSTR(acc_d.account_number, 1, 5) = TO_CHAR(s.ledger_account, 'FM99999999')
      JOIN ds.md_currency_d cur
        ON cur.currency_rk = acc_d.currency_rk
      LEFT JOIN ds.ft_balance_f b
        ON b.account_rk = acc_d.account_rk
       AND b.on_date  = (DATE_TRUNC('month', i_OnDate) - INTERVAL '1 day')
      LEFT JOIN ds.md_exchange_rate_d exch_r
        ON exch_r.currency_rk = acc_d.currency_rk
       AND i_OnDate BETWEEN exch_r.data_actual_date AND exch_r.data_actual_end_date
      LEFT JOIN dm.dm_account_turnover_f at
        ON at.account_rk = acc_d.account_rk
       AND at.on_date BETWEEN DATE_TRUNC('month', i_OnDate) AND (DATE_TRUNC('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')
     WHERE i_OnDate BETWEEN s.start_date AND s.end_date
       AND i_OnDate BETWEEN acc_d.data_actual_date AND acc_d.data_actual_end_date
       AND i_OnDate BETWEEN cur.data_actual_date AND cur.data_actual_end_date
     GROUP BY s.chapter,
           SUBSTR(acc_d.account_number, 1, 5),
           acc_d.char_type, cur.currency_code;
	
	GET DIAGNOSTICS v_RowCount = ROW_COUNT;
    CALL dm.writelog('[END] inserted ' ||  TO_CHAR(v_RowCount, 'FM99999999') || ' rows.', 1);

    COMMIT;
    
END;$$

