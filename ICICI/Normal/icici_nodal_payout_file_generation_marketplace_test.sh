#!/bin/bash
MYSQL=/usr/bin/mysql
DB=mobinew
CURRENTDATE=`date "+%Y-%m-%d"`
TIMESTAMP=`date "+%Y-%m-%d %H-%M-%S"`
COMPRESSDIR=/tmp/cronreports/Payment_Benificiary_ICICI_MBK_Mplac_test_${CURRENTDATE}
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
echo "$query2" | $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' >> ${COMPRESSDIR}/Payment_Benificiary_ICICI_MBK_Mplac_test_$CURRENTDATE.csv
T1=`date "+%Y-%m-%d %H-%M-%S"`
echo "$T1"

cd /tmp/cronreports/ && tar -zcvf Payment_Benificiary_ICICI_MBK_Mplac_test_${CURRENTDATE}.tar.gz Payment_Benificiary_ICICI_MBK_Mplac_test_${CURRENTDATE}

cd /tmp/cronreports/
#ftp -n mbk-ftp.centralindia.cloudapp.azure.com << End
ftp -n 15.207.173.6 << End
user Merchants hwMzZUhtRolr
passive
mkdir ManualTest
cd ManualTest
mkdir ICICI_Test
cd ICICI_Test
mkdir $CURRENTDATE
cd $CURRENTDATE
prompt
binary
hash
put Payment_Benificiary_ICICI_MBK_Mplac_test_${CURRENTDATE}.tar.gz
bye
End
#

#path=${COMPRESSDIR}/Payment_Benificiary_ICICI_MBK_$CURRENTDATE.csv
#curl -H "userId:nocserver@mobikwik.com" -H "Content-Type:multipart/form-data" -F "file=@$path" https://beta2.mobikwik.com/walletapis/merchantpanel/payouts/upload
#ICICI
echo "select wapg.mid,wapg.smid,wapg.orderid,wapg.createdat as txntime,wapg.txnamount as amount,wapg.totalpayoutamt, -(-wapg.totalpayoutamt) as settlement_amount,date_format(now(),'%Y-%m-%d') as settlement_date,wapg.payoutbatchid as settlement_batch,'credit' as type, wapg.wallettxnamount as walletAmount,wapg.pgtxnamount as PgTxnAmount,caSe when wapgm.paymentType ='1' then 'UPI' when wapgm.paymentType in ('2','3') then 'ZIP' else 'WAPG' end aS 'TxnMode', case when collection_mode = '3' then 'Bijlipay_EDC' else 'Mobikwik' end AS Collection_Mode,
(case when payment_instrument='0' then 'Wallet' when payment_instrument='1' then 'WALLET_AND_PG' when payment_instrument='2' then 'PG' when payment_instrument='3' then 'PAYLATER' when payment_instrument='4' then 'ZIP_AND_WALLET' when payment_instrument='5' then 'ZIP_EMI' when payment_instrument='6' then 'UPI' when payment_instrument='7' then 'CC' when payment_instrument='8' then 'DC' when payment_instrument='9' then 'CC_DC' when payment_instrument='10' then 'UPI_COLLECT' when payment_instrument='11' then 'EMANDATE' when payment_instrument='12' then 'NET_BANKING' else null end) AS Payment_Instrument,
mtmd.ext_ref_no AS 'External_Refrence_Number'
   from wallet_as_pg_ledger wapg left join wallet_as_pg_ledger_metadata wapgm on (wapg.id=wapgm.parentid) left join txp t on (t.orderid=wapg.orderid and t.mid=wapg.mid and t.statecode between 28 and 68)
left join merchant_txp_meta_data mtmd on (t.id=mtmd.parent_id) left join merchant mm on(wapg.mid=mm.mid) where (wapg.mid in (select mid from icici_payout_merchants  where isPayoutEnabled=1) or if(wapg.smid is not NULL, wapg.smid in (select mid from icici_payout_merchants  where isPayoutEnabled=1),(wapg.mid in (select mid from icici_payout_merchants  where isPayoutEnabled=1)))) and wapg.payoutbatchid like concat('%',date_format(now(),'%Y%m%d'),'%') and wapg.payoutbatchid IN (SELECT DISTINCT( batch_id ) FROM  settlement_wapg WHERE  status = 'calculated' and created_at >= Curdate()) and ismarketplace ='y' and paymentType not in (2,3,4) and wapg.updatedat>=date(now()) and wapg.settlementdate>=date(now())
union all
select wapg.mid,wapg.smid,wapg.orderid,wapg.createdat as txntime,wapg.txnamount as amount,wapg.txnamount, -wapg.txnamount as settlement_amount,date_format(now(),'%Y-%m-%d') as settlement_date, wapg.refundbatchid as settlement_batch,'debit' as type, wapg.wallettxnamount as walletAmount,wapg.pgtxnamount as PgTxnAmount,caSe when wapgm.paymentType ='1' then 'UPI' when wapgm.paymentType in ('2','3') then 'ZIP' else 'WAPG' end aS 'TxnMode',case when collection_mode = '3' then 'Bijlipay_EDC' else 'Mobikwik' end AS Collection_Mode,
(case when payment_instrument='0' then 'Wallet' when payment_instrument='1' then 'WALLET_AND_PG' when payment_instrument='2' then 'PG' when payment_instrument='3' then 'PAYLATER' when payment_instrument='4' then 'ZIP_AND_WALLET' when payment_instrument='5' then 'ZIP_EMI' when payment_instrument='6' then 'UPI' when payment_instrument='7' then 'CC' when payment_instrument='8' then 'DC' when payment_instrument='9' then 'CC_DC' when payment_instrument='10' then 'UPI_COLLECT' when payment_instrument='11' then 'EMANDATE' when payment_instrument='12' then 'NET_BANKING' else null end) AS Payment_Instrument,
mtmd.ext_ref_no AS 'External_Refrence_Number'  from wallet_as_pg_ledger wapg left join wallet_as_pg_ledger_metadata wapgm on (wapg.id=wapgm.parentid) left join txp t on (t.orderid=wapg.orderid and t.mid=wapg.mid and t.statecode between 28 and 68)
left join merchant_txp_meta_data mtmd on (t.id=mtmd.parent_id)  left join merchant mm on(wapg.mid=mm.mid) where (wapg.mid in (select mid from icici_payout_merchants  where isPayoutEnabled=1) or if(wapg.smid is not NULL, wapg.smid in (select mid from icici_payout_merchants  where isPayoutEnabled=1),(wapg.mid in (select mid from icici_payout_merchants  where isPayoutEnabled=1)))) and wapg.refundbatchid like concat('%',date_format(now(),'%Y%m%d'),'%') and wapg.refundbatchid IN (SELECT DISTINCT( batch_id ) FROM  settlement_wapg WHERE  status = 'calculated' and created_at >= Curdate()) and  ismarketplace='y' and paymentType not in (2,3,4) and wapg.updatedat>=date(now()) and wapg.settlementdate>=date(now())  order by 1"  | $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' >> /tmp/cronreports/working/WAPG_ICICI_Working_file_Mplace_$CURRENTDATE.csv
TIMESTAMP=`date "+%Y-%m-%d %H-%M-%S"`
echo "Count of merchants : $count and Payout Amount :$amount" |  mail -s "Payout Generated for WAPG Payout | Marketplace Merchants | ICICI Bank | $TIMESTAMP" -r noc@mobikwik.com walletops@mobikwik.com merc-common@mobikwik.com merc@mobikwik.com shashank.v@mobikwik.com mpr@mobikwik.com sre@mobikwik.com
gzip  /tmp/cronreports/working/WAPG_ICICI_Working_file_Mplace_test_$CURRENTDATE.csv

cd /tmp/cronreports/working/
#ftp -n mbk-ftp.centralindia.cloudapp.azure.com << End
ftp -n 15.207.173.6 << End
user Merchants hwMzZUhtRolr
passive
mkdir ManualTest
cd ManualTest
mkdir ICICI_Test
cd ICICI_Test
mkdir $CURRENTDATE
cd $CURRENTDATE
prompt
binary
hash
put WAPG_ICICI_Working_file_Mplace_test_$CURRENTDATE.csv.gz
bye
End
echo mail -s "File Generation Completed for ICICI Mplace | Marketplace Merchants | ICICI Bank | $TIMESTAMP" -r noc@mobikwik.com walletops@mobikwik.com merc-common@mobikwik.com merc@mobikwik.com shashank.v@mobikwik.com mpr@mobikwik.com