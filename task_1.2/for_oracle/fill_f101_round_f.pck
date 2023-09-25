create or replace package dm.fill_f101_round_f is
  ----------------------------------------------------------------------------------------------------

  c_MartName                       constant varchar2(30 char) := 'DM.DM_F101_ROUND_F';

  ----------------------------------------------------------------------------------------------------
  /**  –асчет оборотов по счетам за дату
   *   i_OnDate - дата расчета
   */
  procedure fill
  ( i_OnDate                       in date
  );
  ----------------------------------------------------------------------------------------------------

end fill_f101_round_f;
/
create or replace package body dm.fill_f101_round_f is

  ----------------------------------------------------------------------------------------------------
  procedure Log
  ( i_message                      in varchar2
  ) 
  is
  begin
    dm.logger.writeLog('[' || c_MartName || '] ' || i_message);
  end;
  ----------------------------------------------------------------------------------------------------

  ----------------------------------------------------------------------------------------------------
  procedure fill
  ( i_OnDate                       in date
  )
  is
  begin

    Log( '[BEGIN] fill(i_OnDate => date ''' 
         || to_char(i_OnDate, 'yyyy-mm-dd') 
         || ''');'
       );
    
    Log( 'delete on_date = ' 
         || to_char(i_OnDate, 'yyyy-mm-dd')
       );

    delete
      from dm.DM_F101_ROUND_F f
     where trunc(i_OnDate, 'mm')  =  from_date
       and last_day(i_OnDate)    =  to_date;
   
    Log('insert');
   
    insert 
      into dm.dm_f101_round_f
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
    select trunc(i_OnDate, 'mm')                 as from_date,
           last_day(i_OnDate)                    as to_date,
           s.chapter                             as chapter,
           substr(acc_d.account_number, 1, 5)    as ledger_account,
           acc_d.char_type                       as characteristic,
           -- RUB balance
           sum( case 
                  when cur.currency_code in ('643', '810')
                  then b.balance_out
                  else 0
                 end
              )                                  as balance_in_rub,
          -- VAL balance converted to rub
          sum( case 
                 when cur.currency_code not in ('643', '810')
                 then b.balance_out * exch_r.reduced_cource
                 else 0
                end
             )                                   as balance_in_val,
          -- Total: RUB balance + VAL converted to rub
          sum(  case 
                 when cur.currency_code in ('643', '810')
                 then b.balance_out
                 else b.balance_out * exch_r.reduced_cource
               end
             )                                   as balance_in_total  ,
           -- RUB debet turnover
           sum(case 
                 when cur.currency_code in ('643', '810')
                 then at.debet_amount_rub
                 else 0
               end
           )                                     as turn_deb_rub,
           -- VAL debet turnover converted
           sum(case 
                 when cur.currency_code not in ('643', '810')
                 then at.debet_amount_rub
                 else 0
               end
           )                                     as turn_deb_val,
           -- SUM = RUB debet turnover + VAL debet turnover converted
           sum(at.debet_amount_rub)              as turn_deb_total,
           -- RUB credit turnover
           sum(case 
                 when cur.currency_code in ('643', '810')
                 then at.credit_amount_rub
                 else 0
               end
              )                                  as turn_cre_rub,
           -- VAL credit turnover converted
           sum(case 
                 when cur.currency_code not in ('643', '810')
                 then at.credit_amount_rub
                 else 0
               end
              )                                  as turn_cre_val,
           -- SUM = RUB credit turnover + VAL credit turnover converted
           sum(at.credit_amount_rub)             as turn_cre_total,
           cast(null as number)                  as balance_out_rub,
           cast(null as number)                  as balance_out_val,
           cast(null as number)                  as balance_out_total 
      from ds.md_ledger_account_s s
      join ds.md_account_d acc_d
        on substr(acc_d.account_number, 1, 5) = s.ledger_account
      join ds.md_currency_d cur
        on cur.currency_rk = acc_d.currency_rk
      left 
      join ds.ft_balance_f b
        on b.account_rk = acc_d.account_rk
       and b.on_date  = trunc(i_OnDate, 'mm') - 1
      left 
      join ds.md_exchange_rate_d exch_r
        on exch_r.currency_rk = acc_d.currency_rk
       and i_OnDate between exch_r.data_actual_date and exch_r.data_actual_end_date
      left 
      join dm.dm_account_turnover_f at
        on at.account_rk = acc_d.account_rk
       and at.on_date between trunc(i_OnDate, 'mm') and last_day(i_Ondate)
     where i_OnDate between s.start_date and s.end_date
       and i_OnDate between acc_d.data_actual_date and acc_d.data_actual_end_date
       and i_OnDate between cur.data_actual_date and cur.data_actual_end_date
     group by s.chapter,
           substr(acc_d.account_number, 1, 5),
           acc_d.char_type;

    Log('[END] inserted ' || to_char(sql%rowcount) || ' rows.');

    commit;
    
  end;
  ----------------------------------------------------------------------------------------------------

end fill_f101_round_f;
/
