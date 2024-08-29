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
echo "Count of merchants" |  mail -s "Starting payout Generation for WAPG Payout | Non-Marketplace Merchants | ICICI Bank | $TIMESTAMP" noc@mobikwik.com walletops@mobikwik.com merc-common@mobikwik.com merc@mobikwik.com shashank.v@mobikwik.com mpr@mobikwik.com
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
pass
passive
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
echo "select wapg.mid,wapg.smid,wapg.orderid,wapg.createdat as txntime,wapg.txnamount as amount,wapg.totalpayoutamt, -(-wapg.totalpayoutamt) as settlement_amount,date_format(now(),'%Y-%m-%d') as settlement_date,wapg.payoutbatchid as settlement_batch,'credit' as type, wapg.wallettxnamount as walletAmount,wapg.pgtxnamount as PgTxnAmount,case when wapgm.paymentType ='1' then 'UPI' when wapgm.paymentType in ('2','3') then 'ZIP' else 'WAPG' end as 'TxnMode', case when collection_mode = '3' then 'Bijlipay_EDC' else 'Mobikwik' end as Collection_Mode,
(case when payment_instrument='0' then 'Wallet' when payment_instrument='1' then 'WALLET_AND_PG' when payment_instrument='2' then 'PG' when payment_instrument='3' then 'PAYLATER' when payment_instrument='4' then 'ZIP_AND_WALLET' when payment_instrument='5' then 'ZIP_EMI' when payment_instrument='6' then 'UPI' when payment_instrument='7' then 'CC' when payment_instrument='8' then 'DC' when payment_instrument='9' then 'CC_DC' when payment_instrument='10' then 'UPI_COLLECT' when payment_instrument='11' then 'EMANDATE' when payment_instrument='12' then 'NET_BANKING' else null end) as Payment_Instrument,
mtmd.ext_ref_no as 'External_Refrence_Number'
   from wallet_as_pg_ledger wapg
   left join txp t on (t.orderid=wapg.orderid and t.statecode between 28 and 68)
   left join merchant_txp_meta_data mtmd on (t.id=mtmd.parent_id and t.statecode between 28 and 68)
   left join wallet_as_pg_ledger_metadata wapgm on (wapg.id=wapgm.parentid)
   left join merchant on (wapg.mid=merchant.mid ) where (wapg.mid  in (select mid from icici_payout_merchants  where isPayoutEnabled=1) and if(wapg.smid is not  NULL, wapg.smid  in (select mid from icici_payout_merchants  where isPayoutEnabled=1),(wapg.mid in (select mid from icici_payout_merchants  where isPayoutEnabled=1)))) and wapg.updatedat >= DATE(now()) and wapg.payoutbatchid like concat('%',date_format(now(),'%Y%m%d'),'%') and (ismarketplace !='y' or ismarketplace is null) and paymentType not in (2,3,4) and  wapg.payoutbatchid in (select distinct(batch_id) from settlement_wapg where created_at >= curdate() and (status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure')) )
union all
select wapg.mid,wapg.smid,wapg.orderid,wapg.createdat as txntime,wapg.txnamount as amount,wapg.txnamount, -wapg.txnamount as settlement_amount,date_format(now(),'%Y-%m-%d') as settlement_date, wapg.refundbatchid as settlement_batch,'debit' as type, wapg.wallettxnamount as walletAmount,wapg.pgtxnamount as PgTxnAmount,case when wapgm.paymentType ='1' then 'UPI' when wapgm.paymentType in ('2','3') then 'ZIP' else 'WAPG' end as 'TxnMode' , cASe when collection_mode = '3' then 'Bijlipay_EDC' else 'Mobikwik' end AS Collection_Mode,
(cASe when payment_instrument='0' then 'Wallet' when payment_instrument='1' then 'WALLET_AND_PG' when payment_instrument='2' then 'PG' when payment_instrument='3' then 'PAYLATER' when payment_instrument='4' then 'ZIP_AND_WALLET' when payment_instrument='5' then 'ZIP_EMI' when payment_instrument='6' then 'UPI' when payment_instrument='7' then 'CC' when payment_instrument='8' then 'DC' when payment_instrument='9' then 'CC_DC' when payment_instrument='10' then 'UPI_COLLECT' when payment_instrument='11' then 'EMANDATE' when payment_instrument='12' then 'NET_BANKING' else null end) AS Payment_Instrument,
mtmd.ext_ref_no AS 'External_Refrence_Number'  from wallet_as_pg_ledger wapg left join wallet_as_pg_ledger_metadata wapgm on (wapg.id=wapgm.parentid)
 left join txp t on (t.orderid=wapg.orderid and t.statecode between 28 and 68)
 left join merchant_txp_meta_data mtmd on (t.id=mtmd.parent_id)
 left join merchant on (wapg.mid=merchant.mid )  where (wapg.mid  in (select mid from icici_payout_merchants  where isPayoutEnabled=1) and if(wapg.smid is not NULL, wapg.smid  in (select mid from icici_payout_merchants  where isPayoutEnabled=1),(wapg.mid  in (select mid from icici_payout_merchants  where isPayoutEnabled=1)))) and  wapg.updatedat >= DATE(now()) and wapg.refundbatchid like concat('%',date_format(now(),'%Y%m%d'),'%') and (ismarketplace !='y' or ismarketplace is null) and paymentType not in (2,3,4) and wapg.refundbatchid in (select distinct(batch_id) from settlement_wapg where created_at >= curdate() and (status IN ('automated_success', 'automated_failure', 'automated_pending', 'automated_confirm_failure')) ) order by 1"  | $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' >> /tmp/cronreports/working/WAPG_ICICI_Working_file_Non_Mplace_automated_$CURRENTDATE.csv
gzip  /tmp/cronreports/working/WAPG_ICICI_Working_file_Non_Mplace_automated_$CURRENTDATE.csv
cd /tmp/cronreports/
TIMESTAMP=`date "+%Y-%m-%d %H-%M-%S"`
echo "Count of merchants" |  mail -s "Payout Generated for WAPG Payout |  Non-Marketplace Merchants |  ICICI Bank | $TIMESTAMP" noc@mobikwik.com walletops@mobikwik.com merc-common@mobikwik.com merc@mobikwik.com shashank.v@mobikwik.com mpr@mobikwik.com



cd /tmp/cronreports/working/
ftp -n 15.207.173.6 << End
user Merchants hwMzZUhtRolr
pass
passive
mkdir Automated
cd Automated
mkdir ICICI
cd ICICI
mkdir $CURRENTDATE
cd $CURRENTDATE
prompt
binary
hash
put WAPG_ICICI_Working_file_Non_Mplace_automated_$CURRENTDATE.csv.gz
bye
End