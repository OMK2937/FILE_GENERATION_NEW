#!/bin/bash
DB=mobinew
MYSQL="/bin/mysql"

Queries()
{

#AXIS NODAL MP WORKING FILE

echo "
select wapg.mid,
        wapg.smid,
        wapg.orderid,
        wapg.createdat AS txntime,
        wapg.txnamount AS amount,
        wapg.totalpayoutamt,
        -(- wapg.totalpayoutamt) AS settlement_amount,
        date_format(now(), '%Y-%m-%d') AS settlement_date,
        payoutbatchid AS settlement_batch,
        'credit' AS type,
        wapg.wallettxnamount AS walletAmount,
        wapg.pgtxnamount AS PgTxnAmount,
cASe
                when ppo.gatewayid = '15' then 'UPI'
                when ppo.gatewayid = '16' then 'ZIP' else 'WAPG'
        end AS 'TxnMode'
from wallet_as_pg_ledger wapg force index(idx_wallet_as_pg_ledger_updatedat)
        left join wallet_as_pg_ledger_metadata wapgm  on (wapg.id = wapgm.parentid)
        left join pg_payment_order ppo force index(idx_ppo_stamp) on (wapgm.pgorderid = ppo.stamp)
        left join merchant on (wapg.mid = merchant.mid)
where (
                wapg.mid not in (
                        select mid
                        from icici_payout_merchants
                        where isPayoutEnabled = 1 and icici_payout_merchants.mid=wapg.mid
                )
                and if(
                        wapg.smid is not NULL,
                        wapg.smid not in (
                                select mid
                                from icici_payout_merchants
                                where isPayoutEnabled = 1 and icici_payout_merchants.mid=wapg.smid
                        ),
(
                                wapg.mid not in (
                                        select mid
                                        from icici_payout_merchants
                                        where isPayoutEnabled = 1 and icici_payout_merchants.mid=wapg.mid
                                )
                        )
                )
        )
        and wapg.updatedat >= DATE(now())
        and payoutbatchid like concat('%', date_format(now(), '%Y%m%d'), '%')
        and ismarketplace = 'y'
and statecode in (28,38)

        and paymentType not in (2, 3,4)
     AND
    ( (wapg.payoutbatchid, wapg.mid) in (
    select
      batch_id, merchant_id
    from
      settlement_wapg
    where
      created_at >= curdate()
      and status ='calculated'
  )
  OR
  (wapg.payoutbatchid, wapg.smid) in (
    select
      batch_id, merchant_id
    from
      settlement_wapg
    where
      created_at >= curdate()
      and status ='calculated'
  )
    )

        and (
                wapg.mid in (
                        select mid
                        from merchant_payout_config
                        where power_access_file = 1 and merchant_payout_config.mid=wapg.mid
                )
                or wapg.smid in (
                        select mid
                        from merchant_payout_config
                        where power_access_file = 1 and merchant_payout_config.mid=wapg.smid
                )
        )
union all
select wapg.mid,
        wapg.smid,
        wapg.orderid,
        wapg.createdat AS txntime,
        wapg.txnamount AS amount,
        wapg.txnamount,
        - wapg.txnamount AS settlement_amount,
        date_format(now(), '%Y-%m-%d') AS settlement_date,
        wapg.refundbatchid AS settlement_batch,
        'debit' AS type,
        wapg.wallettxnamount AS walletAmount,
        wapg.pgtxnamount AS PgTxnAmount,
cASe
                when ppo.gatewayid = '15' then 'UPI'
                when ppo.gatewayid = '16' then 'ZIP' else 'WAPG'
        end AS 'TxnMode'
from wallet_as_pg_ledger wapg force index(idx_wallet_as_pg_ledger_updatedat)
        left join wallet_as_pg_ledger_metadata wapgm  on (wapg.id = wapgm.parentid)
        left join pg_payment_order ppo force index(idx_ppo_stamp) on (wapgm.pgorderid = ppo.stamp)
        left join merchant force index (merchant_mid_index) on (wapg.mid = merchant.mid)
where (
                wapg.mid not in (
                        select mid
                        from icici_payout_merchants
                        where isPayoutEnabled = 1 and icici_payout_merchants.mid=wapg.mid
                )
                and if(
                        wapg.smid is not NULL,
                        wapg.smid not in (
                                select mid
                                from icici_payout_merchants
                                where isPayoutEnabled = 1 and icici_payout_merchants.mid=wapg.smid
                        ),
(
                                wapg.mid not in (
                                        select mid
                                        from icici_payout_merchants
                                        where isPayoutEnabled = 1 and icici_payout_merchants.mid=wapg.mid
                                )
                        )
                )
        )
        and wapg.updatedat >= DATE(now())
        and refundbatchid like concat('%', date_format(now(), '%Y%m%d'), '%')
        and ismarketplace = 'y'
        and paymentType not in (2, 3,4)
and statecode in (241,243,261,264)
AND
    ( (wapg.refundbatchid, wapg.mid) in (
    select
      batch_id, merchant_id
    from
      settlement_wapg
    where
      created_at >= curdate()
      and status ='calculated'
  )
  OR

  (wapg.refundbatchid, wapg.smid) in (
    select
      batch_id, merchant_id
    from
      settlement_wapg
    where
      created_at >= curdate()
      and status ='calculated'
  )
    )
        and (
                wapg.mid in (
                        select mid
                        from merchant_payout_config
                        where power_access_file = 1 and merchant_payout_config.mid=wapg.mid
                )
                or wapg.smid in (
                        select mid
                        from merchant_payout_config
                        where power_access_file = 1 and merchant_payout_config.mid=wapg.smid
                )
        )
order by 1;
"| $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /data/cronreport-payout/AXIS_NODAL_MP_WORKING_FILE.csv



}

Queries



ftp_upload()
{
todayis=`date "+%F"`
ftp -n -v 15.207.173.6 << EOF
user Merchants hwMzZUhtRolr
pass
passive
mkdir Power_Axis_Payout_New
cd Power_Axis_Payout_New
mkdir $todayis
cd $todayis
prompt
binary
hash
lcd /data/cronreport-payout/
put AXIS_NODAL_MP_WORKING_FILE.csv

bye
EOF
}

ftp_upload








