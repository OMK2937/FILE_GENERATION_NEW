#!/bin/bash
MYSQL=/usr/bin/mysql
DB=mobinew
CURRENTDATE=`date "+%Y-%m-%d"`
TIMESTAMP=`date "+%Y-%m-%d %H-%M-%S"`
COMPRESSDIR=/tmp/cronreports/Payment_Benificiary_ICICI_MBK_Mplac_automated_${CURRENTDATE}
mkdir -p ${COMPRESSDIR}
FILEDATE=date "+%d-%m-%Y"
DATE1DBEFORE=`date --date="1 days ago" "+%Y-%m-%d"`
DATE2DBEFORE=`date --date="2 days ago" "+%Y-%m-%d"`
DATE3DBEFORE=`date --date="3 days ago" "+%Y-%m-%d"`
DATE7DBEFORE=`date --date="7 days ago" "+%Y-%m-%d"`
SED=/bin/sed

echo mail -s "Starting payout File Generation for ICICI Mplace Automated Payout| Marketplace Merchants | ICICI Bank | $TIMESTAMP" -r noc@mobikwik.com walletops@mobikwik.com merc-common@mobikwik.com merc@mobikwik.com shashank.v@mobikwik.com mpr@mobikwik.com

query2="SELECT
  (
    CASE WHEN EXISTS(
      SELECT
        1
      FROM
        bank_holidays
      WHERE
        bank_holidays.date = Curdate()
        OR Dayname(
          Curdate()
        ) = 'Sunday'
        OR (
          Dayname(
            Curdate()
          ) = 'Saturday'
          AND Floor(
            (
              Dayofmonth(
                Curdate()
              ) + 6
            ) / 7
          ) IN (2, 4)
        )
    ) THEN 'I' ELSE 'N' END
  ) AS 'Record Identifier',
  merchant_id AS 'Beneficiary Code',
  Date_format(
    Str_to_date(txn_date, '%d/%m/%Y'),
    '%d-%b-%Y'
  ) AS 'Execution Date',
  amount AS 'Transaction amount',
  Date_format(
    Str_to_date(txn_date, '%d/%m/%Y'),
    '%d-%b-%Y'
  ) AS 'Incoming Credit Date',
  Date_format(
    Str_to_date(txn_date, '%d/%m/%Y'),
    '%d-%b-%Y'
  ) AS 'Transaction Intimation Date',
  batch_id AS 'Additional Info 3',
  CASE WHEN Length(s.accholdername) > 32 THEN Trim(
    LEFT(s.accholdername, 32)
  ) ELSE s.accholdername END AS 'Additional Info 4',
  s.accholdername AS 'Account Name',
  s.enc_accno AS 'Account No.',
  s.enc_ifsc AS 'IFSC Code',
  xtrainvestedamount AS 'XTRA INVESTMENT',
  'ICICI' AS 'Bank type',
  batch_id AS 'Original Batch',
  '' AS 'On Demand Request Id',
  0.0 AS 'Loan Deduction'
FROM
  settlement_wapg sw,
  submerchant s
WHERE
  sw.merchant_id = s.smid
  AND (
    sw.status IN (
      'automated_success', 'automated_failure',
      'automated_pending', 'automated_confirm_failure'
    )
  )
  AND sw.merchant_id IN (
    SELECT
      mid
    FROM
      icici_payout_merchants
    WHERE
      ispayoutenabled = 1
  )
  AND s.splittype = 0
  AND s.enabledforcombinedpayout = 0
  AND batch_id LIKE Concat(
    '%',
    Date_format(Now(), '%Y%m%d'),
    '%'
  )
UNION ALL
SELECT
  (
    CASE WHEN EXISTS(
      SELECT
        1
      FROM
        bank_holidays
      WHERE
        bank_holidays.date = Curdate()
        OR Dayname(
          Curdate()
        ) = 'Sunday'
        OR (
          Dayname(
            Curdate()
          ) = 'Saturday'
          AND Floor(
            (
              Dayofmonth(
                Curdate()
              ) + 6
            ) / 7
          ) IN (2, 4)
        )
    ) THEN 'I' ELSE 'N' END
  ) AS 'Record Identifier',
  s.parentmid AS 'Beneficiary Code',
  Date_format(
    Str_to_date(txn_date, '%d/%m/%Y'),
    '%d-%b-%Y'
  ) AS 'Execution Date',
  Sum(amount) AS 'Transaction amount',
  Date_format(
    Str_to_date(txn_date, '%d/%m/%Y'),
    '%d-%b-%Y'
  ) AS 'Incoming Credit Date',
  Date_format(
    Str_to_date(txn_date, '%d/%m/%Y'),
    '%d-%b-%Y'
  ) AS 'Transaction Intimation Date',
  batch_id AS 'Additional Info 3',
  CASE WHEN Length(s.accholdername) > 32 THEN Trim(
    LEFT(s.accholdername, 32)
  ) ELSE s.accholdername END AS 'Additional Info 4',
  CASE WHEN Length(s.accholdername) > 32 THEN Trim(
    LEFT(s.accholdername, 32)
  ) ELSE s.accholdername END AS 'Account Name',
  s.enc_accno AS 'Account No.',
  s.enc_ifsc AS 'IFSC Code',
  xtrainvestedamount AS 'XTRA INVESTMENT',
  'ICICI' AS 'Bank type',
  batch_id AS 'Original Batch',
  '' AS 'On Demand Request Id',
  0.0 AS 'Loan Deduction'
FROM
  settlement_wapg sw,
  submerchant s,
  merchant m
WHERE
  sw.merchant_id = s.smid
  AND (
    sw.status IN (
      'automated_success', 'automated_failure',
      'automated_pending', 'automated_confirm_failure'
    )
  )
  AND sw.merchant_id IN (
    SELECT
      mid
    FROM
      icici_payout_merchants
    WHERE
      ispayoutenabled = 1
  )
  AND s.splittype = 0
  AND s.enabledforcombinedpayout = 1
  AND batch_id LIKE Concat(
    '%',
    Date_format(Now(), '%Y%m%d'),
    '%'
  )
  AND m.mid = s.parentmid
GROUP BY
  s.parentmid,
  batch_id
UNION ALL
SELECT
  (
    CASE WHEN EXISTS(
      SELECT
        1
      FROM
        bank_holidays
      WHERE
        bank_holidays.date = Curdate()
        OR Dayname(
          Curdate()
        ) = 'Sunday'
        OR (
          Dayname(
            Curdate()
          ) = 'Saturday'
          AND Floor(
            (
              Dayofmonth(
                Curdate()
              ) + 6
            ) / 7
          ) IN (2, 4)
        )
    ) THEN 'I' ELSE 'N' END
  ) AS 'Record Identifier',
  s.parentmid AS 'Beneficiary Code',
  Date_format(
    Str_to_date(txn_date, '%d/%m/%Y'),
    '%d-%b-%Y'
  ) AS 'Execution Date',
  amount AS 'Transaction amount',
  Date_format(
    Str_to_date(txn_date, '%d/%m/%Y'),
    '%d-%b-%Y'
  ) AS 'Incoming Credit Date',
  Date_format(
    Str_to_date(txn_date, '%d/%m/%Y'),
    '%d-%b-%Y'
  ) AS 'Transaction Intimation Date',
  batch_id AS 'Additional Info 3',
  CASE WHEN Length(s.accholdername) > 32 THEN Trim(
    LEFT(s.accholdername, 32)
  ) ELSE s.accholdername END AS 'Additional Info 4',
  CASE WHEN Length(s.accholdername) > 32 THEN Trim(
    LEFT(s.accholdername, 32)
  ) ELSE s.accholdername END AS 'Account Name',
  s.enc_accno AS 'Account No.',
  s.enc_ifsc AS 'IFSC Code',
  xtrainvestedamount AS 'XTRA INVESTMENT',
  'ICICI' AS 'Bank type',
  batch_id AS 'Original Batch',
  '' AS 'On Demand Request Id',
  0.0 AS 'Loan Deduction'
FROM
  settlement_wapg sw,
  submerchant s,
  merchant m
WHERE
  sw.merchant_id = m.mid
  AND (
    sw.status IN (
      'automated_success', 'automated_failure',
      'automated_pending', 'automated_confirm_failure'
    )
  )
  AND sw.merchant_id IN (
    SELECT
      mid
    FROM
      icici_payout_merchants
    WHERE
      ispayoutenabled = 1
  )
  AND s.splittype = 5
  AND sw.batch_id LIKE Concat(
    '%',
    Date_format(Now(), '%Y%m%d'),
    '%'
  )
  AND m.mid = s.parentmid
  AND ismarketplace = 'y'
GROUP BY
  s.parentmid,
  batch_id";
echo "$query2" | $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' >> ${COMPRESSDIR}/Payment_Benificiary_ICICI_MBK_Mplac_automated_$CURRENTDATE.csv
T1=`date "+%Y-%m-%d %H-%M-%S"`
echo "$T1"

cd /tmp/cronreports/ && tar -zcvf Payment_Benificiary_ICICI_MBK_Mplac_automated_${CURRENTDATE}.tar.gz Payment_Benificiary_ICICI_MBK_Mplac_automated_${CURRENTDATE}

cd /tmp/cronreports/
#ftp -n mbk-ftp.centralindia.cloudapp.azure.com << End
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
put Payment_Benificiary_ICICI_MBK_Mplac_automated_${CURRENTDATE}.tar.gz
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
       AND ismarketplace = 'y'
       AND paymenttype NOT IN ( 2, 3, 4 )
       AND wapg.updatedat >= Date(Now())
       AND wapg.settlementdate >= Date(Now())
ORDER  BY 1;"  | $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' >> /tmp/cronreports/working/WAPG_ICICI_Working_file_Mplace_automated_$CURRENTDATE.csv
TIMESTAMP=`date "+%Y-%m-%d %H-%M-%S"`
echo "Count of merchants : $count and Payout Amount :$amount" |  mail -s "Payout Generated for WAPG Payout | Marketplace Merchants | ICICI Bank | $TIMESTAMP" -r noc@mobikwik.com walletops@mobikwik.com merc-common@mobikwik.com merc@mobikwik.com shashank.v@mobikwik.com mpr@mobikwik.com sre@mobikwik.com
gzip  /tmp/cronreports/working/WAPG_ICICI_Working_file_Mplace_automated_$CURRENTDATE.csv

cd /tmp/cronreports/working/
#ftp -n mbk-ftp.centralindia.cloudapp.azure.com << End
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
put WAPG_ICICI_Working_file_Mplace_automated_${CURRENTDATE}.csv.gz
bye
End
echo mail -s "File Generation Completed for ICICI Mplace | Marketplace Merchants | ICICI Bank | $TIMESTAMP" -r noc@mobikwik.com walletops@mobikwik.com merc-common@mobikwik.com merc@mobikwik.com shashank.v@mobikwik.com mpr@mobikwik.com