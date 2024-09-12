#!/bin/bash
MYSQL=/usr/bin/mysql
DB=mobinew
CURRENTDATE=`date "+%Y-%m-%d"`
TIMESTAMP=`date "+%Y-%m-%d %H-%M-%S"`
COMPRESSDIR=/tmp/cronreports/Payment_Benificiary_ICICI_MBK_Non_marketplace_automated_${CURRENTDATE}
mkdir -p ${COMPRESSDIR}
FILEDATE=`date "+%d-%m-%Y"`
DATE1DBEFORE=`date --date="1 days ago" "+%Y-%m-%d"`
DATE2DBEFORE=`date --date="2 days ago" "+%Y-%m-%d"`
DATE3DBEFORE=`date --date="3 days ago" "+%Y-%m-%d"`
DATE7DBEFORE=`date --date="7 days ago" "+%Y-%m-%d"`
SED=/bin/sed
# ICICI Payout File
echo "Count of merchants" |  mail -s "Starting payout Generation for WAPG Payout Automated Payout | Non-Marketplace Merchants | ICICI Bank | $TIMESTAMP" noc@mobikwik.com walletops@mobikwik.com merc-common@mobikwik.com merc@mobikwik.com shashank.v@mobikwik.com mpr@mobikwik.com
query="select (CASE WHEN EXISTS(SELECT 1 FROM bank_holidays WHERE bank_holidays.date = curdate() OR DAYNAME(curdate()) ='Sunday' OR (DAYNAME(curdate()) ='Saturday' AND
FLOOR((DAYOFMONTH(curdate()) + 6 ) / 7) IN (2, 4) )) then 'I' else 'N' end) AS 'Record Identifier', merchant_id AS 'Beneficiary Code',
DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y' ) AS 'Execution Date', amount AS 'Transaction amount',
DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y' ) AS 'Incoming Credit Date',
DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y' ) AS 'Transaction Intimation Date',batch_id AS 'Additional Info 3',
cASe when length(m.accHolderName) > 32 then trim(left(m.accHolderName,32)) else m.accHolderName end AS 'Additional Info 4', m.accHolderName AS'Account Name',
m.enc_accNo AS 'Account No.', m.enc_ifsc AS 'IFSC Code' , xtraInvestedAmount AS 'XTRA INVESTMENT', 'ICICI' AS 'Bank type',batch_id AS 'Original Batch',
'' AS 'On Demand Request Id',
(select COALESCE(sum(pal.amount_adjusted),0.0) from payout_adjustment_ledger pal where sw.id = pal.settlement_id and pal.settlement_type=1) AS 'Loan Deduction'
from settlement_wapg sw, merchant m
where sw.merchant_id = m.mid and sw.created_at>=date(now()) and (sw.status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure'))
and sw.id and sw.merchant_id in (select mid from icici_payout_merchants where isPayoutEnabled=1)
and batch_id like concat('%',date_format(now(),'%Y%m%d'),'%')
 and (ismarketplace is null or ismarketplace!='y') and sw.id not in (
 SELECT req.settlement_id FROM
merchant_settlement_request_metadata req LEFT JOIN merchant_settlement_request od ON
req.request_id = od.id LEFT JOIN settlement_wapg wapg ON
( req.settlement_id = wapg.id AND req.settlement_type = 1 )
WHERE od.created_at >= curdate() and req.settlement_type !=2 )";
echo "$query" | $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' >> ${COMPRESSDIR}/Payment_Benificiary_ICICI_MBK_Non_marketplace_automated_$CURRENTDATE.csv
cd /tmp/cronreports/ && tar -zcvf Payment_Benificiary_ICICI_MBK_Non_marketplace_automated_${CURRENTDATE}.tar.gz Payment_Benificiary_ICICI_MBK_Non_marketplace_automated_${CURRENTDATE}

cd /tmp/cronreports/
ftp -n 15.207.173.6 << End
user Merchants hwMzZUhtRolr
mkdir Automated
cd Automated
mkdir ICICI
cd ICICI
mkdir $CURRENTDATE
cd $CURRENTDATE
prompt
binary
hash
put Payment_Benificiary_ICICI_MBK_Non_marketplace_automated_${CURRENTDATE}.tar.gz
bye
End

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
       LEFT JOIN txp t
              ON ( t.orderid = wapg.orderid
                   AND t.statecode BETWEEN 28 AND 68 )
       LEFT JOIN merchant_txp_meta_data mtmd
              ON ( t.id = mtmd.parent_id
                   AND t.statecode BETWEEN 28 AND 68 )
       LEFT JOIN wallet_as_pg_ledger_metadata wapgm
              ON ( wapg.id = wapgm.parentid )
       LEFT JOIN merchant
              ON ( wapg.mid = merchant.mid )
WHERE  ( wapg.mid IN (SELECT mid
                      FROM   icici_payout_merchants
                      WHERE  ispayoutenabled = 1)
         AND IF(wapg.smid IS NOT NULL,
             wapg.smid IN (SELECT mid
                           FROM   icici_payout_merchants
                           WHERE  ispayoutenabled = 1), (
                 wapg.mid IN (SELECT mid
                              FROM   icici_payout_merchants
                              WHERE  ispayoutenabled = 1) )) )
       AND wapg.updatedat >= Date(Now())
       AND wapg.payoutbatchid LIKE Concat('%', Date_format(Now(), '%Y%m%d'), '%'
                                   )
       AND ( ismarketplace != 'y'
              OR ismarketplace IS NULL )
       AND paymenttype NOT IN ( 2, 3, 4 )
        AND (
        (wapg.payoutbatchid, wapg.mid) in (
        select
        batch_id, merchant_id
        from
        settlement_wapg
        where
        created_at >= curdate()
        and status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure')
        )
        OR
        (wapg.payoutbatchid, wapg.smid) in (
        select
        batch_id, merchant_id
        from
        settlement_wapg
        where
        created_at >= curdate()
        and status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure')
        )
        )
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
                   AND t.statecode BETWEEN 28 AND 68 )
       LEFT JOIN merchant_txp_meta_data mtmd
              ON ( t.id = mtmd.parent_id )
       LEFT JOIN merchant
              ON ( wapg.mid = merchant.mid )
WHERE  ( wapg.mid IN (SELECT mid
                      FROM   icici_payout_merchants
                      WHERE  ispayoutenabled = 1)
         AND IF(wapg.smid IS NOT NULL,
             wapg.smid IN (SELECT mid
                           FROM   icici_payout_merchants
                           WHERE  ispayoutenabled = 1), (
                 wapg.mid IN (SELECT mid
                              FROM   icici_payout_merchants
                              WHERE  ispayoutenabled = 1) )) )
       AND wapg.updatedat >= Date(Now())
       AND wapg.refundbatchid LIKE Concat('%', Date_format(Now(), '%Y%m%d'), '%'
                                   )
       AND ( ismarketplace != 'y'
              OR ismarketplace IS NULL )
       AND paymenttype NOT IN ( 2, 3, 4 )
        AND (
        (wapg.refundbatchid, wapg.mid) in (
        select
        batch_id, merchant_id
        from
        settlement_wapg
        where
        created_at >= curdate()
        and status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure')
        )
        OR
        (wapg.refundbatchid, wapg.smid) in (
        select
        batch_id, merchant_id
        from
        settlement_wapg
        where
        created_at >= curdate()
        and status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure')
        )
        )

ORDER  BY 1;"  | $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' >> /tmp/cronreports/working/WAPG_ICICI_Working_file_Non_Mplace_automated_$CURRENTDATE.csv
gzip  /tmp/cronreports/working/WAPG_ICICI_Working_file_Non_Mplace_automated_$CURRENTDATE.csv
cd /tmp/cronreports/
TIMESTAMP=`date "+%Y-%m-%d %H-%M-%S"`
echo "Count of merchants" |  mail -s "Payout Generated for WAPG Payout |  Non-Marketplace Merchants |  ICICI Bank | $TIMESTAMP" noc@mobikwik.com walletops@mobikwik.com merc-common@mobikwik.com merc@mobikwik.com shashank.v@mobikwik.com mpr@mobikwik.com



cd /tmp/cronreports/working/
ftp -n 15.207.173.6 << End
user Merchants hwMzZUhtRolr
mkdir Automated
cd Automated
mkdir ICICI
cd ICICI
mkdir $CURRENTDATE
cd $CURRENTDATE
prompt
binary
hash
put WAPG_ICICI_Working_file_Non_Mplace_automated_${CURRENTDATE}.csv.gz
bye
End