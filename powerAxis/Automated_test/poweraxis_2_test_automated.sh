Queries()
{
#AXIS NODAL MP WORKING FILE
echo "
SELECT wapg.mid,
       wapg.smid,
       wapg.orderid,
       wapg.createdat                 AS txntime,
       wapg.txnamount                 AS amount,
       wapg.totalpayoutamt,
       -( -wapg.totalpayoutamt )      AS settlement_amount,
       Date_format(Now(), '%Y-%m-%d') AS settlement_date,
       payoutbatchid                  AS settlement_batch,
       'credit'                       AS type,
       wapg.wallettxnamount           AS walletAmount,
       wapg.pgtxnamount               AS PgTxnAmount,
       CASE
         WHEN ppo.gatewayid = '15' THEN 'UPI'
         WHEN ppo.gatewayid = '16' THEN 'ZIP'
         ELSE 'WAPG'
       end                            AS 'TxnMode'
FROM   wallet_as_pg_ledger wapg FORCE INDEX(idx_wallet_as_pg_ledger_updatedat)
       LEFT JOIN wallet_as_pg_ledger_metadata wapgm ON ( wapg.id = wapgm.parentid )
       LEFT JOIN pg_payment_order ppo ON ( wapgm.pgorderid = ppo.stamp )
       LEFT JOIN merchant ON ( wapg.mid = merchant.mid )
       LEFT JOIN icici_payout_merchants ipm1 ON wapg.mid = ipm1.mid AND ipm1.ispayoutenabled = 1
       LEFT JOIN icici_payout_merchants ipm2 ON wapg.smid = ipm2.mid AND ipm2.ispayoutenabled = 1
WHERE  ipm1.mid IS NULL
       AND ipm2.mid IS NULL
       AND wapg.updatedat >= Date(Now())
       AND payoutbatchid LIKE Concat('%', Date_format(Now(), '%Y%m%d'), '%')
       AND ismarketplace = 'y'
       AND paymenttype NOT IN ( 2, 3, 4 )
       AND payoutbatchid IN (select distinct(batch_id) from settlement_wapg
                where created_at >= curdate() AND status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure'))
       AND ( wapg.mid IN (SELECT mid
                          FROM   merchant_payout_config
                          WHERE  power_access_file = 1)
              OR wapg.smid IN (SELECT mid
                               FROM   merchant_payout_config
                               WHERE  power_access_file = 1) )
UNION ALL
SELECT wapg.mid,
       wapg.smid,
       wapg.orderid,
       wapg.createdat                 AS txntime,
       wapg.txnamount                 AS amount,
       wapg.txnamount,
       -wapg.txnamount                AS settlement_amount,
       Date_format(Now(), '%Y-%m-%d') AS settlement_date,
       wapg.refundbatchid             AS settlement_batch,
       'debit'                        AS type,
       wapg.wallettxnamount           AS walletAmount,
       wapg.pgtxnamount               AS PgTxnAmount,
       CASE
         WHEN ppo.gatewayid = '15' THEN 'UPI'
         WHEN ppo.gatewayid = '16' THEN 'ZIP'
         ELSE 'WAPG'
       end                            AS 'TxnMode'
FROM   wallet_as_pg_ledger wapg FORCE INDEX(idx_wallet_as_pg_ledger_updatedat)
       LEFT JOIN wallet_as_pg_ledger_metadata wapgm ON ( wapg.id = wapgm.parentid )
       LEFT JOIN pg_payment_order ppo ON ( wapgm.pgorderid = ppo.stamp )
       LEFT JOIN merchant ON ( wapg.mid = merchant.mid )
       LEFT JOIN icici_payout_merchants ipm1 ON wapg.mid = ipm1.mid AND ipm1.ispayoutenabled = 1
       LEFT JOIN icici_payout_merchants ipm2 ON wapg.smid = ipm2.mid AND ipm2.ispayoutenabled = 1
WHERE  ipm1.mid IS NULL
       AND ipm2.mid IS NULL
       AND wapg.updatedat >= Date(Now())
       AND refundbatchid LIKE Concat('%', Date_format(Now(), '%Y%m%d'), '%')
       AND ismarketplace = 'y'
       AND paymenttype NOT IN ( 2, 3, 4 )
       AND refundbatchid IN (select distinct(batch_id) from settlement_wapg
                where created_at >= curdate() AND status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure'))
       AND ( wapg.mid IN (SELECT mid
                          FROM   merchant_payout_config
                          WHERE  power_access_file = 1)
              OR wapg.smid IN (SELECT mid
                               FROM   merchant_payout_config
                               WHERE  power_access_file = 1) )
ORDER  BY 1;
"| $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /data/cronreport-payout/AXIS_NODAL_MP_WORKING_FILE_test.csv


}

Queries


ftp_upload()
{
todayis=`date "+%F"`
ftp -n -v 15.207.173.6 << EOF
user Merchants hwMzZUhtRolr
pass
passive
mkdir Automated_Test
cd Automated_Test
mkdir PowerAxis_Automated_test
cd PowerAxis_Automated_test
mkdir PowerAxisSRE_Automated_test
cd PowerAxisSRE_Automated_test
mkdir $todayis
cd $todayis
prompt
binary
hash
lcd /data/cronreport-payout/
put AXIS_NODAL_MP_WORKING_FILE_test.csv

bye
EOF
}

ftp_upload

bash -xf /var/scripts/check_emptyfiles_New.sh >> /tmp/cronlogs/check_emptyfiles.log
