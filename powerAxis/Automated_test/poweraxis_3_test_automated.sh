echo -e "\n\n\n\n\nScript execution started:"
date
echo -e "\n\n"

Queries()
{


#AXIS ESCROW MP WORKING FILE

echo "
select
  t.smid,
  t.orderid,
  t.parentmid,
  w.txnamount,
  t.memberid,
  w.totalfee,
  w.totalservicetax,
  w.totalpayoutamt,
  w.payoutbatchid AS 'payoutbatchid',
  '' AS 'refundbatchid',
  w.createdat,
  w.settlementdate AS payoutdate,
  '' AS 'refund amount',
  '' AS 'refund adjusted date',
  'Payout initiated' AS 'Status',
  date(w.settlementdate) AS SettlementDate,
  t.id AS txid,
  'Credit' AS 'settlementtype',
 -(- w.totalpayoutamt) AS 'settlementamount',
  cASe when partnerTDR IN (1, 2)
  and paymentType not in (2, 3, 4) THEN 'WAPG Global Payout' ELSE 'Normal Payout' END AS 'Payout Type',
  cASe when partnerTDR IN (1, 2)
  and paymentType not in (2, 3, 4) THEN 'MBK54671' when partnerTDR IN (1, 2)
  and paymentType = 4 THEN 'INTEROP_WALLET' ELSE 'NA' END AS 'Global MID',
  cASe when collection_mode = '3' then 'Bijlipay_EDC' else 'Mobikwik' end AS Collection_Mode,
  (
    cASe when payment_instrument = '0' then 'Wallet' when payment_instrument = '1' then 'WALLET_AND_PG' when payment_instrument = '2' then 'PG' when payment_instrument = '3' then 'PAYLATER' when payment_instrument = '4' then 'ZIP_AND_WALLET' when payment_instrument = '5' then 'ZIP_EMI' when payment_instrument = '6' then 'UPI' when payment_instrument = '7' then 'CC' when payment_instrument = '8' then 'DC' when payment_instrument = '9' then 'CC_DC' when payment_instrument = '10' then 'UPI_COLLECT' when payment_instrument = '11' then 'EMANDATE' when payment_instrument = '12' then 'NET_BANKING' else null end
  ) AS Payment_Instrument,
  mtmd.ext_ref_no AS 'External_Refrence_Number',
  cASe when t.member_uid = '85861954'
  and paymenttype = 4 then 'ThirdPartyUPI' when t.member_uid != '85861954'
  and paymenttype = '4' then 'MobikwikUPI' else NULL end UPI_MODE
from
  txpmarketplace t
  left join merchant_txp_meta_data mtmd on (t.id = mtmd.parent_id)
  left join wallet_as_pg_ledger w on (
    t.orderid = w.orderid
    and t.parentmid = w.mid
    and w.statecode in (28, 38)
  )
  left join wallet_as_pg_ledger_metadata wm on (w.id = wm.parentid)
where
  w.settlementdate >= date(now())
  and w.updatedat >= date(now())
  and w.isnodalprocessed = 1
  and paymenttype = 4
  and partnertdr in (1, 2)
  and t.parentmid not in ('MBK5778')
  and (
    t.parentmid in (
      select
        mid
      from
        merchant_payout_config
      where
        power_access_file = 1
    )
    or t.smid in (
      select
        mid
      from
        merchant_payout_config
      where
        power_access_file = 1
    )
  )
  and w.payoutbatchid in (
    select
      distinct(batch_id)
    from
      kotak_settlement
    where
      created_at >= curdate()
      and status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure')
  ) and t.statecode between 28 and 68
UNION ALL
select
  t.smid,
  t.orderid,
  t.parentmid,
  t.txnamount,
  t.memberid,
  t.fee,
  t.servicetax,
  t.payoutamt,
  t.payoutbatchid AS 'payoutbatchid',
  '' AS 'refundbatchid',
  t.createdat,
  c.createdat AS payoutdate,
  '' AS 'refund amount',
  '' AS 'refund adjusted date',
  'Payout initiated' AS 'Status',
  date(c.createdat) AS SettlementDate,
  t.id AS txid,
  'Credit' AS 'settlementtype',
  cASe when partnertdr in (1, 2)
  and paymentType != 2 then -(- t.txnamount) else -(- payoutamt) end AS 'settlementamount',
  cASe when partnerTDR IN (1, 2)
  and paymentType != 2 THEN 'WAPG Global Payout' ELSE 'Normal Payout' END AS 'Payout Type',
  cASe when partnerTDR IN (1, 2)
  and paymentType != 2 THEN 'MBK54671' when partnerTDR IN (1, 2)
  and paymentType = 2 THEN 'ZIP_WALLET' ELSE 'NA' END AS 'Global MID',
  cASe when collection_mode = '3' then 'Bijlipay_EDC' else 'Mobikwik' end AS Collection_Mode,
  (
    cASe when payment_instrument = '0' then 'Wallet' when payment_instrument = '1' then 'WALLET_AND_PG' when payment_instrument = '2' then 'PG' when payment_instrument = '3' then 'PAYLATER' when payment_instrument = '4' then 'ZIP_AND_WALLET' when payment_instrument = '5' then 'ZIP_EMI' when payment_instrument = '6' then 'UPI' when payment_instrument = '7' then 'CC' when payment_instrument = '8' then 'DC' when payment_instrument = '9' then 'CC_DC' when payment_instrument = '10' then 'UPI_COLLECT' when payment_instrument = '11' then 'EMANDATE' when payment_instrument = '12' then 'NET_BANKING' else null end
  ) AS Payment_Instrument,
  mtmd.ext_ref_no AS 'External_Refrence_Number',
  NULL UPI_MODE
from
  txpmarketplace t
  left join txcmarketplace c on (
    t.id = c.parentid
    and c.statecode = 35
  )
  left join merchant_txp_meta_data mtmd on (t.id = mtmd.parent_id)
  left join wallet_as_pg_ledger w on (
    t.orderid = w.orderid
    and t.parentmid = w.mid
    and w.statecode in (28, 68)
  )
  left join wallet_as_pg_ledger_metadata wm on (w.id = wm.parentid)
where
  t.parentmid not in ('MBK5778', 'MBK5778A')
  and t.payoutbatchid like concat(
    '%',
    date_format (now(), '%Y%m%d'),
    '%'
  )
  and t.statecode >= 35
  and t.statecode <= 68
  and t.updatedat > date(now())
  and (
    cASe when partnertdr in (1, 2)
    and paymentType != 2 then 'MBK54671' in (
      select
        mid
      from
        payout_merchants_info
      where
        payoutBankName = 'axis'
        and isPayoutEnabled = 1
        and mid = 'MBK54671'
    ) else t.parentmid in (
      select
        mid
      from
        payout_merchants_info
      where
        payoutBankName = 'axis'
        and isPayoutEnabled = 1
    )
    or t.smid in (
      select
        mid
      from
        payout_merchants_info
      where
        payoutBankName = 'axis'
        and isPayoutEnabled = 1
    ) end
  )
  and t.payoutbatchid in (
    select
      distinct(batch_id)
    from
      kotak_settlement
    where
      created_at >= curdate()
      and status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure')
  )
  and (
    t.parentmid in (
      select
        mid
      from
        merchant_payout_config
      where
        power_access_file = 1
    )
    or t.smid in (
      select
        mid
      from
        merchant_payout_config
      where
        power_access_file = 1
    )
  )
union
select
  tx.smid,
  tx.orderid,
  tx.parentmid,
  tx.txnamount,
  tx.memberid,
  tx.fee,
  tx.servicetax,
  tx.payoutamt,
  '' AS 'payoutbatchid',
  tx.refundbatchid AS 'refundbatchid',
  tx.createdat,
  '' AS payoutdate,
  tx.txnamount AS 'refund amount',
  c1.createdat AS 'refund adjusted date',
  'Refund Adjusted' AS 'Status',
  date(c1.createdat) AS SettlementDate,
  tx.id AS txid,
  'Debit' AS 'settlementtype',
  - tx.txnamount AS 'settlementamount',
  cASe when partnerTDR IN (1, 2)
  and paymentType != 2 THEN 'WAPG Global Payout' ELSE 'Normal Payout' END AS 'Payout Type',
  cASe when partnerTDR IN (1, 2)
  and paymentType != 2 THEN 'MBK54671' when partnerTDR IN (1, 2)
  and paymentType = 2 THEN 'ZIP_WALLET' ELSE 'NA' END AS 'Global MID',
  cASe when collection_mode = '3' then 'Bijlipay_EDC' else 'Mobikwik' end AS Collection_Mode,
  (
    cASe when payment_instrument = '0' then 'Wallet' when payment_instrument = '1' then 'WALLET_AND_PG' when payment_instrument = '2' then 'PG' when payment_instrument = '3' then 'PAYLATER' when payment_instrument = '4' then 'ZIP_AND_WALLET' when payment_instrument = '5' then 'ZIP_EMI' when payment_instrument = '6' then 'UPI' when payment_instrument = '7' then 'CC' when payment_instrument = '8' then 'DC' when payment_instrument = '9' then 'CC_DC' when payment_instrument = '10' then 'UPI_COLLECT' when payment_instrument = '11' then 'EMANDATE' when payment_instrument = '12' then 'NET_BANKING' else null end
  ) AS Payment_Instrument,
  mtmd.ext_ref_no AS 'External_Refrence_Number',
  NULL UPI_MODE
from
  txpmarketplace tx
  join txcmarketplace c1 on (
    tx.id = c1.parentid
    and c1.statecode in (44, 45)
  )
  left join merchant_txp_meta_data mtmd on (tx.id = mtmd.parent_id)
  left join wallet_as_pg_ledger w on (
    tx.orderid = w.orderid
    and tx.parentmid = w.mid
    and w.statecode in (28, 68)
  )
  left join wallet_as_pg_ledger_metadata wm on (w.id = wm.parentid)
where
  tx.parentmid not in ('MBK5778', 'MBK5778A')
  and tx.refundbatchid like concat(
    '%',
    date_format (now(), '%Y%m%d'),
    '%'
  )
  and cASe when tx.partnerTDR IN (1, 2) then tx.statecode in (44, 45, 37) else tx.statecode in (44, 45) end
  and tx.updatedat > date(now())
  and (
    cASe when partnertdr in (1, 2)
    and paymentType != 2 then 'MBK54671' in (
      select
        mid
      from
        payout_merchants_info
      where
        payoutBankName = 'axis'
        and isPayoutEnabled = 1
        and mid = 'MBK54671'
    ) else tx.parentmid in (
      select
        mid
      from
        payout_merchants_info
      where
        payoutBankName = 'axis'
        and isPayoutEnabled = 1
    )
    or tx.smid in (
      select
        mid
      from
        payout_merchants_info
      where
        payoutBankName = 'axis'
        and isPayoutEnabled = 1
    ) end
  )
  and tx.refundbatchid in (
    select
      distinct(batch_id)
    from
      kotak_settlement
    where
      created_at >= curdate()
      and status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure')
  )
  and (
    tx.parentmid in (
      select
        mid
      from
        merchant_payout_config
      where
        power_access_file = 1
    )
    or tx.smid in (
      select
        mid
      from
        merchant_payout_config
      where
        power_access_file = 1
    )
  )
UNION all
select
  t.smid,
  t.orderid,
  t.parentmid,
  t.txnamount,
  t.memberid,
  t.fee,
  t.servicetax,
  t.payoutamt,
  '' AS 'payoutbatchid',
  t.refundbatchid AS 'refundbatchid',
  t.createdat,
  '' AS payoutdate,
  c1.amount AS 'refund amount',
  c1.createdat AS 'refund adjusted date',
  'partial Refund Paid' AS 'Status',
  date(c1.createdat) AS SettlementDate,
  t.id AS txid,
  'Debit' AS 'settlementtype',
  - c1.amount AS 'settlementamount',
  cASe when partnerTDR IN (1, 2)
  and paymentType != 2 THEN 'WAPG Global Payout' ELSE 'Normal Payout' END AS 'Payout Type',
  cASe when partnerTDR IN (1, 2)
  and paymentType != 2 THEN 'MBK54671' when partnerTDR IN (1, 2)
  and paymentType = 2 THEN 'ZIP_WALLET' ELSE 'NA' END AS 'Global MID',
  cASe when collection_mode = '3' then 'Bijlipay_EDC' else 'Mobikwik' end AS Collection_Mode,
  (
    cASe when payment_instrument = '0' then 'Wallet' when payment_instrument = '1' then 'WALLET_AND_PG' when payment_instrument = '2' then 'PG' when payment_instrument = '3' then 'PAYLATER' when payment_instrument = '4' then 'ZIP_AND_WALLET' when payment_instrument = '5' then 'ZIP_EMI' when payment_instrument = '6' then 'UPI' when payment_instrument = '7' then 'CC' when payment_instrument = '8' then 'DC' when payment_instrument = '9' then 'CC_DC' when payment_instrument = '10' then 'UPI_COLLECT' when payment_instrument = '11' then 'EMANDATE' when payment_instrument = '12' then 'NET_BANKING' else null end
  ) AS Payment_Instrument,
  mtmd.ext_ref_no AS 'External_Refrence_Number',
  NULL UPI_MODE
from
  txpmarketplace t
  left join txcmarketplace c1 on (
    t.id = c1.parentid
    and c1.statecode = 46
  )
  left join merchant_txp_meta_data mtmd on (t.id = mtmd.parent_id)
  left join wallet_as_pg_ledger w on (
    t.orderid = w.orderid
    and t.parentmid = w.mid
    and w.statecode in (28, 68)
  )
  left join wallet_as_pg_ledger_metadata wm on (w.id = wm.parentid)
where
  t.parentmid not in ('MBK5778', 'MBK5778A')
  and c1.rrn like concat(
    '%',
    date_format (now(), '%Y%m%d'),
    '%'
  )
  and t.statecode >= 28
  and t.statecode < 69
  and c1.statecode = 46
  and t.updatedat > date(now())
  and (
    cASe when partnertdr in (1, 2)
    and paymentType != 2 then 'MBK54671' in (
      select
        mid
      from
        payout_merchants_info
      where
        payoutBankName = 'axis'
        and isPayoutEnabled = 1
        and mid = 'MBK54671'
    ) else t.parentmid in (
      select
        mid
      from
        payout_merchants_info
      where
        payoutBankName = 'axis'
        and isPayoutEnabled = 1
    ) end
  )
  and t.refundbatchid in (
    select
      distinct(batch_id)
    from
      kotak_settlement
    where
      created_at >= curdate()
      and status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure')
  )
  and (
    t.parentmid in (
      select
        mid
      from
        merchant_payout_config
      where
        power_access_file = 1
    )
    or t.smid in (
      select
        mid
      from
        merchant_payout_config
      where
        power_access_file = 1
    )
  )
union all
select
  t.smid,
  t.orderid,
  t.parentmid,
  t.txnamount,
  t.memberid,
  t.fee,
  t.servicetax,
  t.payoutamt,
  '' AS 'payoutbatchid',
  c1.rrn AS 'refundbatchid',
  t.createdat,
  '' AS payoutdate,
  c1.amount AS 'refund amount',
  c1.createdat AS 'refund adjusted date',
  'Partial Refund Adjusted' AS 'Status',
  date(c1.createdat) AS SettlementDate,
  t.id AS txid,
  'Debit' AS 'settlementtype',
  - c1.amount AS 'settlementamount',
  cASe when partnerTDR IN (1, 2)
  and paymentType != 2 THEN 'WAPG Global Payout' ELSE 'Normal Payout' END AS 'Payout Type',
  cASe when partnerTDR IN (1, 2)
  and paymentType != 2 THEN 'MBK54671' when partnerTDR IN (1, 2)
  and paymentType = 2 THEN 'ZIP_WALLET' ELSE 'NA' END AS 'Global MID',
  cASe when collection_mode = '3' then 'Bijlipay_EDC' else 'Mobikwik' end AS Collection_Mode,
  (
    cASe when payment_instrument = '0' then 'Wallet' when payment_instrument = '1' then 'WALLET_AND_PG' when payment_instrument = '2' then 'PG' when payment_instrument = '3' then 'PAYLATER' when payment_instrument = '4' then 'ZIP_AND_WALLET' when payment_instrument = '5' then 'ZIP_EMI' when payment_instrument = '6' then 'UPI' when payment_instrument = '7' then 'CC' when payment_instrument = '8' then 'DC' when payment_instrument = '9' then 'CC_DC' when payment_instrument = '10' then 'UPI_COLLECT' when payment_instrument = '11' then 'EMANDATE' when payment_instrument = '12' then 'NET_BANKING' else null end
  ) AS Payment_Instrument,
  mtmd.ext_ref_no AS 'External_Refrence_Number',
  NULL UPI_MODE
from
  txpmarketplace t
  left join txcmarketplace c1 on (
    t.id = c1.parentid
    and c1.statecode = 66
  )
  left join merchant_txp_meta_data mtmd on (t.id = mtmd.parent_id)
  left join wallet_as_pg_ledger w on (
    t.orderid = w.orderid
    and t.parentmid = w.mid
    and w.statecode in (28, 68)
  )
  left join wallet_as_pg_ledger_metadata wm on (w.id = wm.parentid)
where
  t.parentmid not in ('MBK5778', 'MBK5778A')
  and c1.rrn like concat(
    '%',
    date_format (now(), '%Y%m%d'),
    '%'
  )
  and (
    t.statecode >= 60
    and t.statecode <= 68
  )
  and t.updatedat > date(now())
  and (
    cASe when partnertdr in (1, 2)
    and paymentType != 2 then 'MBK54671' in (
      select
        mid
      from
        payout_merchants_info
      where
        payoutBankName = 'axis'
        and isPayoutEnabled = 1
        and mid = 'MBK54671'
    ) else t.parentmid in (
      select
        mid
      from
        payout_merchants_info
      where
        payoutBankName = 'axis'
        and isPayoutEnabled = 1
    ) end
  )
  and t.refundbatchid in (
    select
      distinct(batch_id)
    from
      kotak_settlement
    where
      created_at >= curdate()
      and status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure')
  )
  and (
    t.parentmid in (
      select
        mid
      from
        merchant_payout_config
      where
        power_access_file = 1
    )
    or t.smid in (
      select
        mid
      from
        merchant_payout_config
      where
        power_access_file = 1
    )
  )
UNION ALL
select
  t.smid,
  t.orderid,
  t.parentmid,
  w.txnamount,
  t.memberid,
  w.totalfee,
  w.totalservicetax,
  w.totalpayoutamt,
  '' AS 'payoutbatchid',
  w.refundbatchid AS 'refundbatchid',
  w.createdat,
  '' AS payoutdate,
  w.txnamount AS 'refund amount',
  w.createdat AS 'refund adjusted date',
  'Partial Refund Adjusted' AS 'Status',
  date(w.createdat) AS SettlementDate,
  t.id AS txid,
  'Debit' AS 'settlementtype',
  - w.txnamount AS 'settlementamount',
  cASe when partnerTDR IN (1, 2)
  and paymentType not in (2, 3, 4) THEN 'WAPG Global Payout' ELSE 'Normal Payout' END AS 'Payout Type',
  cASe when partnerTDR IN (1, 2)
  and paymentType not in (2, 3, 4) THEN 'MBK54671' when partnerTDR IN (1, 2)
  and paymentType = 4 THEN 'INTEROP_WALLET' ELSE 'NA' END AS 'Global MID',
  cASe when collection_mode = '3' then 'Bijlipay_EDC' else 'Mobikwik' end AS Collection_Mode,
  (
    cASe when payment_instrument = '0' then 'Wallet' when payment_instrument = '1' then 'WALLET_AND_PG' when payment_instrument = '2' then 'PG' when payment_instrument = '3' then 'PAYLATER' when payment_instrument = '4' then 'ZIP_AND_WALLET' when payment_instrument = '5' then 'ZIP_EMI' when payment_instrument = '6' then 'UPI' when payment_instrument = '7' then 'CC' when payment_instrument = '8' then 'DC' when payment_instrument = '9' then 'CC_DC' when payment_instrument = '10' then 'UPI_COLLECT' when payment_instrument = '11' then 'EMANDATE' when payment_instrument = '12' then 'NET_BANKING' else null end
  ) AS Payment_Instrument,
  mtmd.ext_ref_no AS 'External_Refrence_Number',
  cASe when t.member_uid = '85861954'
  and paymenttype = 4 then 'ThirdPartyUPI' when t.member_uid != '85861954'
  and paymenttype = '4' then 'MobikwikUPI' else NULL end UPI_MODE
from
  txpmarketplace t
  left join merchant_txp_meta_data mtmd on (t.id = mtmd.parent_id)
  left join wallet_as_pg_ledger w on (
    t.orderid = w.orderid
    and t.parentmid = w.mid
    and w.statecode > 200
  )
  left join wallet_as_pg_ledger_metadata wm on (w.id = wm.parentid)
where
  w.settlementdate >= date(now())
  and w.updatedat >= date(now())
  and w.isnodalprocessed = 1
  and paymenttype = 4
  and partnertdr in (1, 2)
  and t.parentmid not in ('MBK5778')
  and (
    t.parentmid in (
      select
        mid
      from
        merchant_payout_config
      where
        power_access_file = 1
    )
    or t.smid in (
      select
        mid
      from
        merchant_payout_config
      where
        power_access_file = 1
    )
  )
  and w.refundbatchid in (
    select
      distinct(batch_id)
    from
      kotak_settlement
    where
      created_at >= curdate()
      and status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure')) and t.statecode between 28 and 68;
"| $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /data/cronreport-payout/AXIS_ESCORW_MP_WORKING_FILE_test.csv



}

Queries

echo -e "\nBefore executing the FTP Function..\n"
ls -lrth /data/cronreport-payout/AXIS_ESCORW_MP_WORKING_FILE_test.csv
date
echo -e "\n\n"

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
put AXIS_ESCORW_MP_WORKING_FILE_test.csv

bye
EOF
}

ftp_upload

echo -e " \nFTP Function completed successfully.\n"
date
echo -e "\n\n"

#bash -xf /var/scripts/check_emptyfiles_New.sh >> /tmp/cronlogs/check_emptyfiles.log