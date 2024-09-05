select wapg.mid,
       wapg.smid,
       wapg.orderid,
       wapg.createdat AS txntime,
       wapg.txnamount AS amount,
       wapg.totalpayoutamt,
       -(-wapg.totalpayoutamt) AS settlement_amount,
       date_format(now(), '%Y-%m-%d') AS settlement_date,
       payoutbatchid AS settlement_batch,
       'credit' AS type,
       wapg.wallettxnamount AS walletAmount,
       wapg.pgtxnamount AS PgTxnAmount,
       CASE
           WHEN ppo.gatewayid = '15' THEN 'UPI'
           WHEN ppo.gatewayid = '16' THEN 'ZIP'
           ELSE 'WAPG'
       END AS 'TxnMode'
from wallet_as_pg_ledger wapg
         force index (idx_wallet_as_pg_ledger_updatedat)
         left join wallet_as_pg_ledger_metadata wapgm on wapg.id = wapgm.parentid
         left join pg_payment_order ppo force index (idx_ppo_stamp) on wapgm.pgorderid = ppo.stamp
         left join merchant on wapg.mid = merchant.mid
         left join icici_payout_merchants ipm on wapg.mid = ipm.mid
                                              or wapg.smid = ipm.mid
                                              and ipm.isPayoutEnabled = 1
where ipm.mid IS NULL
  and wapg.updatedat >= DATE(now())
  and payoutbatchid like concat('%', date_format(now(), '%Y%m%d'), '%')
  and ismarketplace = 'y'
  and statecode in (28, 38)
  and paymentType not in (2, 3, 4)
  and payoutbatchid in (select distinct(batch_id)
                        from settlement_wapg
                        where created_at >= curdate()
                          and status = 'calculated')
  and (wapg.mid in (select mid
                    from merchant_payout_config
                    where power_access_file = 1)
       or wapg.smid in (select mid
                        from merchant_payout_config
                        where power_access_file = 1))
union all
select wapg.mid,
       wapg.smid,
       wapg.orderid,
       wapg.createdat AS txntime,
       wapg.txnamount AS amount,
       wapg.txnamount,
       -wapg.txnamount AS settlement_amount,
       date_format(now(), '%Y-%m-%d') AS settlement_date,
       wapg.refundbatchid AS settlement_batch,
       'debit' AS type,
       wapg.wallettxnamount AS walletAmount,
       wapg.pgtxnamount AS PgTxnAmount,
       CASE
           WHEN ppo.gatewayid = '15' THEN 'UPI'
           WHEN ppo.gatewayid = '16' THEN 'ZIP'
           ELSE 'WAPG'
       END AS 'TxnMode'
from wallet_as_pg_ledger wapg
         force index (idx_wallet_as_pg_ledger_updatedat)
         left join wallet_as_pg_ledger_metadata wapgm on wapg.id = wapgm.parentid
         left join pg_payment_order ppo force index (idx_ppo_stamp) on wapgm.pgorderid = ppo.stamp
         left join merchant force index (merchant_mid_index) on wapg.mid = merchant.mid
         left join icici_payout_merchants ipm on wapg.mid = ipm.mid
                                              or wapg.smid = ipm.mid
                                              and ipm.isPayoutEnabled = 1
where ipm.mid IS NULL
  and wapg.updatedat >= DATE(now())
  and refundbatchid like concat('%', date_format(now(), '%Y%m%d'), '%')
  and ismarketplace = 'y'
  and paymentType not in (2, 3, 4)
  and statecode in (241, 243, 261, 264)
  and refundbatchid in (select distinct(batch_id)
                        from settlement_wapg
                        where created_at >= curdate()
                          and status = 'calculated')
  and (wapg.mid in (select mid
                    from merchant_payout_config
                    where power_access_file = 1)
       or wapg.smid in (select mid
                        from merchant_payout_config
                        where power_access_file = 1))

order by 1;
