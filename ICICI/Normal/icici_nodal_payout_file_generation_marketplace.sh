#!/bin/bash
MYSQL=/usr/bin/mysql
DB=mobinew
CURRENTDATE=`date "+%Y-%m-%d"`
TIMESTAMP=`date "+%Y-%m-%d %H-%M-%S"`
COMPRESSDIR=/tmp/cronreports/Payment_Benificiary_ICICI_MBK_Mplac${CURRENTDATE}
mkdir -p ${COMPRESSDIR}
FILEDATE=date "+%d-%m-%Y"
DATE1DBEFORE=`date --date="1 days ago" "+%Y-%m-%d"`
DATE2DBEFORE=`date --date="2 days ago" "+%Y-%m-%d"`
DATE3DBEFORE=`date --date="3 days ago" "+%Y-%m-%d"`
DATE7DBEFORE=`date --date="7 days ago" "+%Y-%m-%d"`
SED=/bin/sed

echo mail -s "Starting payout File Generation for ICICI Mplace | Marketplace Merchants | ICICI Bank | $TIMESTAMP" -r noc@mobikwik.com walletops@mobikwik.com merc-common@mobikwik.com merc@mobikwik.com shashank.v@mobikwik.com mpr@mobikwik.com

query2="select (CASE WHEN EXISTS(SELECT 1 FROM bank_holidays WHERE bank_holidays.date = curdate() OR DAYNAME(curdate()) ='Sunday' OR (DAYNAME(curdate()) ='Saturday' AND FLOOR((DAYOFMONTH(curdate()) + 6 ) / 7) IN (2, 4) )) then 'I' else 'N' end) as 'Record Identifier', merchant_id as 'Beneficiary Code', DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y' ) as 'Execution Date', amount as 'Transaction amount', DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y' ) as 'Incoming Credit Date',DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y' ) as 'Transaction Intimation Date',batch_id as 'Additional Info 3', case when length(s.accHolderName) > 32 then trim(left(s.accHolderName,32)) else s.accHolderName end as 'Additional Info 4' , s.accHolderName as 'Account Name', s.enc_accNo as 'Account No.', s.enc_ifsc as 'IFSC Code', xtraInvestedAmount as 'XTRA INVESTMENT', 'ICICI' as 'Bank type',batch_id as 'Original Batch', '' as 'On Demand Request Id', 0.0 as 'Loan Deduction' from settlement_wapg sw, submerchant s where sw.merchant_id = s.smid and sw.status = 'calculated' and  sw.merchant_id in (select mid from icici_payout_merchants  where isPayoutEnabled=1) and s.splittype = 0 and s.enabledForCombinedPayout = 0 and batch_id like concat('%',date_format(now(),'%Y%m%d'),'%')
union all
select (CASE WHEN EXISTS(SELECT 1 FROM bank_holidays WHERE bank_holidays.date = curdate() OR DAYNAME(curdate()) ='Sunday' OR (DAYNAME(curdate()) ='Saturday' AND FLOOR((DAYOFMONTH(curdate()) + 6 ) / 7) IN (2, 4) )) then 'I' else 'N' end) as 'Record Identifier', s.parentmid as 'Beneficiary Code', DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y' ) as 'Execution Date', sum(amount) as 'Transaction amount', DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y' ) as 'Incoming Credit Date',DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y' ) as 'Transaction Intimation Date',batch_id as 'Additional Info 3', case when length(s.accHolderName) > 32 then trim(left(s.accHolderName,32)) else s.accHolderName end as 'Additional Info 4',case when length(s.accHolderName) > 32 then trim(left(s.accHolderName,32)) else s.accHolderName end as 'Account Name', s.enc_accNo as 'Account No.', s.enc_ifsc as 'IFSC Code', xtraInvestedAmount as 'XTRA INVESTMENT', 'ICICI' as 'Bank type',batch_id as 'Original Batch', '' as 'On Demand Request Id', 0.0 as 'Loan Deduction' from settlement_wapg sw, submerchant s, merchant m where sw.merchant_id = s.smid and sw.status = 'calculated' and  sw.merchant_id in (select mid from icici_payout_merchants  where isPayoutEnabled=1) and s.splittype = 0 and s.enabledForCombinedPayout = 1 and batch_id like concat('%',date_format(now(),'%Y%m%d'),'%') and m.mid = s.parentmid group by s.parentmid,batch_id
union all
select (CASE WHEN EXISTS(SELECT 1 FROM bank_holidays WHERE bank_holidays.date = curdate() OR DAYNAME(curdate()) ='Sunday' OR (DAYNAME(curdate()) ='Saturday' AND FLOOR((DAYOFMONTH(curdate()) + 6 ) / 7) IN (2, 4) )) then 'I' else 'N' end) as 'Record Identifier', s.parentmid as 'Beneficiary Code', DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y' ) as 'Execution Date', amount as 'Transaction amount', DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y' ) as 'Incoming Credit Date',DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y' ) as 'Transaction Intimation Date',batch_id as 'Additional Info 3', case when length(s.accHolderName) > 32 then trim(left(s.accHolderName,32)) else s.accHolderName end as 'Additional Info 4',case when length(s.accHolderName) > 32 then trim(left(s.accHolderName,32)) else s.accHolderName end as 'Account Name', s.enc_accNo as 'Account No.', s.enc_ifsc as 'IFSC Code', xtraInvestedAmount as 'XTRA INVESTMENT', 'ICICI' as 'Bank type',batch_id as 'Original Batch', '' as 'On Demand Request Id', 0.0 as 'Loan Deduction' from settlement_wapg sw, submerchant s,merchant m where sw.merchant_id = m.mid and sw.status = 'calculated' and sw.merchant_id in (select mid from icici_payout_merchants  where isPayoutEnabled=1) and  s.splittype = 5 and sw.batch_id like concat('%',date_format(now(),'%Y%m%d'),'%') and m.mid = s.parentmid and ismarketplace='y' group by s.parentmid,batch_id";
echo "$query2" | $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' >> ${COMPRESSDIR}/Payment_Benificiary_ICICI_MBK_Mplac$CURRENTDATE.csv
T1=`date "+%Y-%m-%d %H-%M-%S"`
echo "$T1"

cd /tmp/cronreports/ && tar -zcvf Payment_Benificiary_ICICI_MBK_Mplac${CURRENTDATE}.tar.gz Payment_Benificiary_ICICI_MBK_Mplac${CURRENTDATE}

cd /tmp/cronreports/
#ftp -n mbk-ftp.centralindia.cloudapp.azure.com << End
ftp -n 15.207.173.6 << End
user Merchants hwMzZUhtRolr
passive
cd makepayoutfile_wapg_report
mkdir $CURRENTDATE
cd $CURRENTDATE
prompt
binary
hash
put Payment_Benificiary_ICICI_MBK_Mplac${CURRENTDATE}.tar.gz
bye
End
#

#path=${COMPRESSDIR}/Payment_Benificiary_ICICI_MBK_$CURRENTDATE.csv
#curl -H "userId:nocserver@mobikwik.com" -H "Content-Type:multipart/form-data" -F "file=@$path" https://beta2.mobikwik.com/walletapis/merchantpanel/payouts/upload
#ICICI
echo "SELECT wapg.mid,
       wapg.smid,
       wapg.orderid,
       wapg.createdat                 AS txntime,
       wapg.txnamount                 AS amount,
       wapg.totalpayoutamt,
       -( -wapg.totalpayoutamt )      AS settlement_amount,
       Date_format(Now(), '%Y-%m-%d') AS settlement_date,
       wapg.payoutbatchid             AS settlement_batch,
       'credit'                       AS type,
       wapg.wallettxnamount           AS walletAmount,
       wapg.pgtxnamount               AS PgTxnAmount,
       CASE
         WHEN wapgm.paymenttype = '1' THEN 'UPI'
         WHEN wapgm.paymenttype IN ( '2', '3' ) THEN 'ZIP'
         ELSE 'WAPG'
       end                            AS 'TxnMode',
       CASE
         WHEN collection_mode = '3' THEN 'Bijlipay_EDC'
         ELSE 'Mobikwik'
       end                            AS Collection_Mode,
       ( CASE
           WHEN payment_instrument = '0' THEN 'Wallet'
           WHEN payment_instrument = '1' THEN 'WALLET_AND_PG'
           WHEN payment_instrument = '2' THEN 'PG'
           WHEN payment_instrument = '3' THEN 'PAYLATER'
           WHEN payment_instrument = '4' THEN 'ZIP_AND_WALLET'
           WHEN payment_instrument = '5' THEN 'ZIP_EMI'
           WHEN payment_instrument = '6' THEN 'UPI'
           WHEN payment_instrument = '7' THEN 'CC'
           WHEN payment_instrument = '8' THEN 'DC'
           WHEN payment_instrument = '9' THEN 'CC_DC'
           WHEN payment_instrument = '10' THEN 'UPI_COLLECT'
           WHEN payment_instrument = '11' THEN 'EMANDATE'
           WHEN payment_instrument = '12' THEN 'NET_BANKING'
           ELSE NULL
         end )                        AS Payment_Instrument,
       mtmd.ext_ref_no                AS 'External_Refrence_Number'
FROM   wallet_as_pg_ledger wapg
       LEFT JOIN wallet_as_pg_ledger_metadata wapgm
              ON ( wapg.id = wapgm.parentid )
       LEFT JOIN txp t
              ON ( t.orderid = wapg.orderid
                   AND t.mid = wapg.mid
                   AND t.statecode BETWEEN 28 AND 68 )
       LEFT JOIN merchant_txp_meta_data mtmd
              ON ( t.id = mtmd.parent_id )
       LEFT JOIN merchant mm
              ON( wapg.mid = mm.mid )
WHERE  ( wapg.mid IN (SELECT mid
                      FROM   icici_payout_merchants
                      WHERE  ispayoutenabled = 1)
          OR IF(wapg.smid IS NOT NULL,
             wapg.smid IN (SELECT mid
                           FROM   icici_payout_merchants
                           WHERE  ispayoutenabled = 1), (
                 wapg.mid IN (SELECT mid
                              FROM   icici_payout_merchants
                              WHERE  ispayoutenabled = 1) )) )
       AND wapg.payoutbatchid LIKE Concat('%', Date_format(Now(), '%Y%m%d'), '%')
        AND (
        (wapg.payoutbatchid, wapg.mid) in (
        select
        batch_id, merchant_id
        from
        settlement_wapg
        where
        created_at >= curdate()
        and status = 'calculated'
        )
        OR
        (wapg.payoutbatchid, wapg.smid) in (
        select
        batch_id, merchant_id
        from
        settlement_wapg
        where
        created_at >= curdate()
        and status = 'calculated'
        )
        )
       AND ismarketplace = 'y'
       AND paymenttype NOT IN ( 2, 3, 4 )
       AND wapg.updatedat >= Date(Now())
       AND wapg.settlementdate >= Date(Now())
UNION
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
         WHEN wapgm.paymenttype = '1' THEN 'UPI'
         WHEN wapgm.paymenttype IN ( '2', '3' ) THEN 'ZIP'
         ELSE 'WAPG'
       end                            AS 'TxnMode',
       CASE
         WHEN collection_mode = '3' THEN 'Bijlipay_EDC'
         ELSE 'Mobikwik'
       end                            AS Collection_Mode,
       ( CASE
           WHEN payment_instrument = '0' THEN 'Wallet'
           WHEN payment_instrument = '1' THEN 'WALLET_AND_PG'
           WHEN payment_instrument = '2' THEN 'PG'
           WHEN payment_instrument = '3' THEN 'PAYLATER'
           WHEN payment_instrument = '4' THEN 'ZIP_AND_WALLET'
           WHEN payment_instrument = '5' THEN 'ZIP_EMI'
           WHEN payment_instrument = '6' THEN 'UPI'
           WHEN payment_instrument = '7' THEN 'CC'
           WHEN payment_instrument = '8' THEN 'DC'
           WHEN payment_instrument = '9' THEN 'CC_DC'
           WHEN payment_instrument = '10' THEN 'UPI_COLLECT'
           WHEN payment_instrument = '11' THEN 'EMANDATE'
           WHEN payment_instrument = '12' THEN 'NET_BANKING'
           ELSE NULL
         end )                        AS Payment_Instrument,
       mtmd.ext_ref_no                AS 'External_Refrence_Number'
FROM   wallet_as_pg_ledger wapg
       LEFT JOIN wallet_as_pg_ledger_metadata wapgm
              ON ( wapg.id = wapgm.parentid )
       LEFT JOIN txp t
              ON ( t.orderid = wapg.orderid
                   AND t.mid = wapg.mid
                   AND t.statecode BETWEEN 28 AND 68 )
       LEFT JOIN merchant_txp_meta_data mtmd
              ON ( t.id = mtmd.parent_id )
       LEFT JOIN merchant mm
              ON( wapg.mid = mm.mid )
WHERE  ( wapg.mid IN (SELECT mid
                      FROM   icici_payout_merchants
                      WHERE  ispayoutenabled = 1)
          OR IF(wapg.smid IS NOT NULL,
             wapg.smid IN (SELECT mid
                           FROM   icici_payout_merchants
                           WHERE  ispayoutenabled = 1), (
                 wapg.mid IN (SELECT mid
                              FROM   icici_payout_merchants
                              WHERE  ispayoutenabled = 1) )) )
        AND wapg.refundbatchid LIKE Concat('%', Date_format(Now(), '%Y%m%d'), '%')
        AND (
        (wapg.refundbatchid, wapg.mid) in (
        select
        batch_id, merchant_id
        from
        settlement_wapg
        where
        created_at >= curdate()
        and status = 'calculated'
        )
        OR
        (wapg.refundbatchid, wapg.smid) in (
        select
        batch_id, merchant_id
        from
        settlement_wapg
        where
        created_at >= curdate()
        and status = 'calculated'
        )
        )
       AND ismarketplace = 'y'
       AND paymenttype NOT IN ( 2, 3, 4 )
       AND wapg.updatedat >= Date(Now())
       AND wapg.settlementdate >= Date(Now())
ORDER  BY 1;"  | $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' >> /tmp/cronreports/working/WAPG_ICICI_Working_file_Mplace_$CURRENTDATE.csv
TIMESTAMP=`date "+%Y-%m-%d %H-%M-%S"`
echo "Count of merchants : $count and Payout Amount :$amount" |  mail -s "Payout Generated for WAPG Payout | Marketplace Merchants | ICICI Bank | $TIMESTAMP" -r noc@mobikwik.com walletops@mobikwik.com merc-common@mobikwik.com merc@mobikwik.com shashank.v@mobikwik.com mpr@mobikwik.com sre@mobikwik.com
gzip  /tmp/cronreports/working/WAPG_ICICI_Working_file_Mplace_$CURRENTDATE.csv

cd /tmp/cronreports/working/
#ftp -n mbk-ftp.centralindia.cloudapp.azure.com << End
ftp -n 15.207.173.6 << End
user Merchants hwMzZUhtRolr
passive
cd makepayoutfile_wapg_report
cd  $CURRENTDATE
prompt
binary
hash
put WAPG_ICICI_Working_file_Mplace_${CURRENTDATE}.csv.gz
bye
End
echo mail -s "File Generation Completed for ICICI Mplace | Marketplace Merchants | ICICI Bank | $TIMESTAMP" -r noc@mobikwik.com walletops@mobikwik.com merc-common@mobikwik.com merc@mobikwik.com shashank.v@mobikwik.com mpr@mobikwik.com