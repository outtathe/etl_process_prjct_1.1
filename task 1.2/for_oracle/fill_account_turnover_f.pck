create or replace package dm.fill_account_turnover_f is
  ----------------------------------------------------------------------------------------------------
  c_MartName                       constant varchar2(30 char) := 'DM.DM_ACCOUNT_TURNOVER_F';

  ----------------------------------------------------------------------------------------------------
  /**  –асчет оборотов по счетам за дату
   *   i_OnDate - дата расчета
   */
  procedure fill
  ( i_OnDate                       in date
  );
  ----------------------------------------------------------------------------------------------------

end fill_account_turnover_f;
/
create or replace package body dm.fill_account_turnover_f is
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
      from dm.dm_account_turnover_f f
     where f.on_date = i_OnDate;
   
    Log('insert');
    insert
      into dm.dm_account_turnover_f
           ( on_date
           , account_rk
           , credit_amount
           , credit_amount_rub
           , debet_amount
           , debet_amount_rub
           )
    with wt_turn as
    ( select p.credit_account_rk                  as account_rk
           , p.credit_amount                      as credit_amount
           , p.credit_amount 
             * nvl(er.reduced_cource, 1)          as credit_amount_rub
           , cast(null as number)                 as debet_amount
           , cast(null as number)                 as debet_amount_rub
        from ds.ft_posting_f p
        join ds.md_account_d a
          on a.account_rk = p.credit_account_rk
        left
        join ds.md_exchange_rate_d er
          on er.currency_rk = a.currency_rk
         and i_OnDate between er.data_actual_date   and er.data_actual_end_date
       where p.oper_date = i_OnDate
         and i_OnDate           between a.data_actual_date    and a.data_actual_end_date
         and a.data_actual_date between trunc(i_OnDate, 'mm') and last_day(i_OnDate)
       union all
      select p.debet_account_rk                   as account_rk
           , cast(null as number)                 as credit_amount
           , cast(null as number)                 as credit_amount_rub
           , p.debet_amount                       as debet_amount
           , p.debet_amount 
             * nvl(er.reduced_cource, 1)          as debet_amount_rub
        from ds.ft_posting_f p
        join ds.md_account_d a
          on a.account_rk = p.debet_account_rk
        left 
        join ds.md_exchange_rate_d er
          on er.currency_rk = a.currency_rk
         and i_OnDate between er.data_actual_date and er.data_actual_end_date
       where p.oper_date = i_OnDate
         and i_OnDate           between a.data_actual_date and a.data_actual_end_date
         and a.data_actual_date between trunc(i_OnDate, 'mm') and last_day(i_OnDate)
    )
    select i_OnDate                               as on_date
         , t.account_rk
         , sum(t.credit_amount)                   as credit_amount
         , sum(t.credit_amount_rub)               as credit_amount_rub
         , sum(t.debet_amount)                    as debet_amount
         , sum(t.debet_amount_rub)                as debet_amount_rub
      from wt_turn t
     group by t.account_rk;

    Log('[END] inserted ' || to_char(sql%rowcount) || ' rows.');

    commit;
    
  end;
  ----------------------------------------------------------------------------------------------------

end fill_account_turnover_f;
/
