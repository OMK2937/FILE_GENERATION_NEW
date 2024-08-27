Queries()
{



#AXIS NODAL MP WORKING FILE
echo "
select wapg.mid,
        wapg.smid,
        wapg.orderid,
        wapg.createdat as txntime,
        wapg.txnamount as amount,
        wapg.totalpayoutamt,
        -(- wapg.totalpayoutamt) as settlement_amount,
        date_format(now(), '%Y-%m-%d') as settlement_date,
        payoutbatchid as settlement_batch,
        'credit' as type,
        wapg.wallettxnamount as walletAmount,
        wapg.pgtxnamount as PgTxnAmount,
caSe
                when ppo.gatewayid = '15' then 'UPI'
                when ppo.gatewayid = '16' then 'ZIP' else 'WAPG'
        end aS 'TxnMode'
from wallet_as_pg_ledger wapg force index(idx_wallet_as_pg_ledger_updatedat)
        left join wallet_as_pg_ledger_metadata wapgm force index(PRIMARY) on (wapg.id = wapgm.parentid)
        left join pg_payment_order ppo force index(idx_ppo_stamp) on (wapgm.pgorderid = ppo.stamp)
        left join merchant on (wapg.mid = merchant.mid)
where (
                wapg.mid not in (
                        select mid
                        from icici_payout_merchants
                        where isPayoutEnabled = 1
                )
                and if(
                        wapg.smid is not NULL,
                        wapg.smid not in (
                                select mid
                                from icici_payout_merchants
                                where isPayoutEnabled = 1
                        ),
(
                                wapg.mid not in (
                                        select mid
                                        from icici_payout_merchants
                                        where isPayoutEnabled = 1
                                )
                        )
                )
        )
        and wapg.updatedat >= DATE(now())
        and payoutbatchid like concat('%', date_format(now(), '%Y%m%d'), '%')
        and ismarketplace = 'y'
        and paymentType not in (2, 3,4)
        and payoutbatchid in (
                select distinct(batch_id)
                from settlement_wapg
                where status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure') AND created_at >= curdate()
        )
        and (
                wapg.mid in (
                        select mid
                        from merchant_payout_config
                        where power_access_file = 1
                )
                or wapg.smid in (
                        select mid
                        from merchant_payout_config
                        where power_access_file = 1
                )
        )
union all
select wapg.mid,
        wapg.smid,
        wapg.orderid,
        wapg.createdat as txntime,
        wapg.txnamount as amount,
        wapg.txnamount,
        - wapg.txnamount as settlement_amount,
        date_format(now(), '%Y-%m-%d') as settlement_date,
        wapg.refundbatchid as settlement_batch,
        'debit' as type,
        wapg.wallettxnamount as walletAmount,
        wapg.pgtxnamount as PgTxnAmount,
caSe
                when ppo.gatewayid = '15' then 'UPI'
                when ppo.gatewayid = '16' then 'ZIP' else 'WAPG'
        end aS 'TxnMode'
from wallet_as_pg_ledger wapg force index(idx_wallet_as_pg_ledger_updatedat)
        left join wallet_as_pg_ledger_metadata wapgm force index(PRIMARY) on (wapg.id = wapgm.parentid)
        left join pg_payment_order ppo force index(idx_ppo_stamp) on (wapgm.pgorderid = ppo.stamp)
        left join merchant on (wapg.mid = merchant.mid)
where (
                wapg.mid not in (
                        select mid
                        from icici_payout_merchants
                        where isPayoutEnabled = 1
                )
                and if(
                        wapg.smid is not NULL,
                        wapg.smid not in (
                                select mid
                                from icici_payout_merchants
                                where isPayoutEnabled = 1
                        ),
(
                                wapg.mid not in (
                                        select mid
                                        from icici_payout_merchants
                                        where isPayoutEnabled = 1
                                )
                        )
                )
        )
        and wapg.updatedat >= DATE(now())
        and refundbatchid like concat('%', date_format(now(), '%Y%m%d'), '%')
        and ismarketplace = 'y'
        and paymentType not in (2, 3,4)
        and refundbatchid in (
                select distinct(batch_id)
                from settlement_wapg
                where status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure') and created_at >= curdate()
        )
        and (
                wapg.mid in (
                        select mid
                        from merchant_payout_config
                        where power_access_file = 1
                )
                or wapg.smid in (
                        select mid
                        from merchant_payout_config
                        where power_access_file = 1
                )
        )
order by 1;
"| mysql -u mobinewcronmstr01 -p'C@da5u#643' -h mbk-payout-replica.clztcamsjaiy.ap-south-1.rds.amazonaws.com -D mobinew -A -P 3308 | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /data/cronreport-payout/AXIS_NODAL_MP_WORKING_FILE_automated.csv


}

Queries


ftp_upload()
{
todayis=`date "+%F"`
ftp -n -v 15.207.173.6 << EOF
user Merchants hwMzZUhtRolr
pass
passive
mkdir Automated
cd Automated
mkdir PowerAxis_Automated
cd PowerAxis_Automated
mkdir PowerAxisSRE_Automated
cd PowerAxisSRE_Automated
mkdir $todayis
cd $todayis
prompt
binary
hash
lcd /data/cronreport-payout/
put AXIS_NODAL_MP_WORKING_FILE_automated.csv

bye
EOF
}

ftp_upload

bash -xf /var/scripts/check_emptyfiles_New.sh >> /tmp/cronlogs/check_emptyfiles.log
