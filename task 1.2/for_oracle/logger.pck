create or replace package dm.logger is

  -------------------------------------------------------------------------------------------------
  log_NOTICE            constant int := 1;
  log_WARNING           constant int := 2;
  log_ERROR             constant int := 3;
  log_DEBUG             constant int := 4;

  c_splitToTable        constant int := 4000;
  c_splitToDbmsOutput   constant int := 900;

  -------------------------------------------------------------------------------------------------
  /**
   * writeLog - запись в таблицу журнала dm.lg_messages
   * Parameters: i_message           - Сообщение.
   *             i_messageType       - Тип сообщения ( dm.log.log_NOTICE  - примечание,
   *                                                   dm.log.log_WARNING - предупреждение,
   *                                                   dm.log.log_ERROR   - сообщение об ошибке,
   *                                                   dm.log.log_DEBUG   - отладочное сообщение
   *                                                 ).
   */
  procedure writeLog
  ( i_message           in varchar2
  , i_messageType       in int := log_NOTICE
  );
  -------------------------------------------------------------------------------------------------
end logger;
/
create or replace package body dm.logger is

  ---------------------------------------------------------------------------------------------------
  procedure writeLog
  ( i_message           in varchar2
  , i_messageType       in int := log_NOTICE
  )
  is
    pragma autonomous_transaction;
    v_logDate           timestamp;
    v_callerType        varchar2(100 char);
    v_callerOwner       varchar2(100 char);
    v_caller            varchar2(100 char);
    v_line              number;
    v_message           varchar2(32767 char);
  begin
    v_logDate := systimestamp;
    owa_util.who_called_me(v_callerOwner, v_caller, v_line, v_callerType);

    -- split to log table
    v_message := i_message;
    while length(v_message) > 0 loop

      insert
        into dm.lg_messages
             ( record_id
             , date_time
             , sid
             , message
             , message_type
             , caller_type
             , caller_owner
             , caller
             , line
             , serial
             , username
             , osuser
             , machine
             , program
             , module
             , action
             , logon_time
             )
      select dm.seq_lg_messages.nextval
           , systimestamp
           , vs.sid
           , substr(v_message, 1, c_splitToTable)
           , i_messageType
           , v_callerType
           , v_callerOwner
           , v_caller
           , v_line
           , vs.serial#
           , vs.username
           , vs.osuser
           , vs.machine
           , vs.program
           , vs.module
           , vs.action
           , vs.logon_time
        from v$session vs
       where vs.sid = ( select sid
                          from v$mystat
                         where rownum = 1
                      );
      v_message := substr(v_message, c_splitToTable + 1);
    end loop;

    commit;

    dbms_output.enable(500000);

    -- split to dbms output
    v_message := i_message;
    while length(v_message) > 0 loop
      dbms_output.put_line( to_char(v_logDate, 'dd.mm.yyyy HH24:MI:SS.FF')
                            || ' '
                            || i_messageType
                            || ' ['
                            || v_callerOwner
                            || '.'
                            || v_caller
                            || '('
                            || v_line
                            || ')'
                            || '] '
                            || '> '
                            || substr(v_message, 1, c_splitToDbmsOutput)
                          );
      v_message := substr(v_message, c_splitToDbmsOutput + 1);
    end loop;

  end writeLog;
  ---------------------------------------------------------------------------------------------------
end logger;
/
