#!bin/bash
MYSQL=/usr/bin/mysql
DB=mobinew

Queries()
{

#This file contains :
#AXIS ESCROW MP PAYOUT FILE | Merchant
#AXIS ESCROW MP PAYOUT FILE | SubMerchant
#AXIS NODAL MP PAYOUT FILE
#AXIS NODAL NMP PAYOUT FILE
#AXIS NODAL NMP WORKING FILE
#AXIS ESCROW NMP WORKING FILE
#AXIS ESCROW NMP PAYOUT FILE --- present





query="select 'txp' as Table_name,
        MAX(CREATEDAT) AS MAX_CREATEDAT,
        MAX(UPDATEDAT) AS MAX_UPDATEDAT
from mobinew.txp
UNION
select 'txpmarketplace' as Table_name,
        MAX(CREATEDAT) AS MAX_CREATEDAT,
        MAX(UPDATEDAT) AS MAX_UPDATEDAT
from mobinew.txpmarketplace
UNION
select 'txc' as Table_name,
        MAX(CREATEDAT) AS MAX_CREATEDAT,
        'NA' AS MAX_UPDATEDAT
from mobinew.txc
UNION
select 'txcmarketplace' as Table_name,
        MAX(CREATEDAT) AS MAX_CREATEDAT,
        'NA' AS MAX_UPDATEDAT
from mobinew.txcmarketplace
UNION
select 'wallet_as_pg_ledger' as Table_name,
        MAX(CREATEDAT) AS MAX_CREATEDAT,
	MAX(UPDATEDAT) AS MAX_UPDATEDAT
from mobinew.wallet_as_pg_ledger
UNION
select 'wallet_as_pg_ledger_metadata' as Table_name,
        MAX(CREATEDAT) AS MAX_CREATEDAT,
        MAX(UPDATEDAT) AS MAX_UPDATEDAT
from mobinew.wallet_as_pg_ledger_metadata ";

echo "$query" | $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /data/cronreport-payout/Tables_timestamp_data.csv


ftp_upload()
{
todayis=`date "+%F"`
ftp -n -v 15.207.173.6 << EOF
user Merchants hwMzZUhtRolr
pass
passive
mkdir ManualTest
cd ManaulTest
mkdir PowerAxisSRE_Test
cd PowerAxisSRE_Test
mkdir $todayis
cd $todayis
prompt
binary
hash
lcd /data/cronreport-payout/
put Tables_timestamp_data.csv

bye
EOF
}

ftp_upload


#AXIS ESCROW NMP PAYOUT FILE

echo "select (
                CASE
                        WHEN EXISTS(
                                SELECT 1
                                FROM bank_holidays
                                WHERE bank_holidays.date = curdate()
                                        OR DAYNAME(curdate()) = 'Sunday'
                                        OR (
                                                DAYNAME(curdate()) = 'Saturday'
                                                AND FLOOR((DAYOFMONTH(curdate()) + 6) / 7) IN (2, 4)
                                        )
                        ) then 'I' else 'N'
                end
        ) as 'Record Identifier',
        merchant_id as 'Beneficiary Code',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Execution Date',
        amount as 'Transaction amount',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Incoming Credit Date',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Transaction Intimation Date',
        batch_id as 'Additional Info 3',
        case
                when length(m.accHolderName) > 32 then trim(left(m.accHolderName, 32)) else m.accHolderName
        end as 'Additional Info 4',
        m.accHolderName as 'Account Name',
        m.enc_accNo as 'Account No.',
        m.enc_ifsc as 'IFSC Code',
        xtraInvestedAmount as 'XTRA INVESTMENT',
        'AXIS_ESCROW' as 'Bank type',
        batch_id as 'Original Batch',
        '' as 'On Demand Request Id',
        (
                select COALESCE(pal.amount_adjusted, 0.0)
                from payout_adjustment_ledger pal
                where ks.id = pal.settlement_id
                        and pal.settlement_type = 2
        ) as 'Loan Deduction'
from kotak_settlement ks,
        merchant m
where ks.merchant_id = m.mid
        and batch_id like concat('%', date_format(now(), '%Y%m%d'), '%')
        and ks.status ='calculated' 
        and isXtraInvestmentMerchant = 0
        and (
                ismarketplace is null
                or ismarketplace != 'y'
        )
        and txn_date = date_format(now(), '%d/%m/%Y')
        and batch_id not in (
                Select distinct(batch_id)
                from merchant_settlement_request
                where created_at >= CURDATE()
        )
        and mid in (
                select mid
                from merchant_payout_config
                where power_access_file = 1
        );
"| $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /data/cronreport-payout/AXIS_ESCROW_NMP_PAYOUT_FILE_test.csv

ftp_upload()
{
todayis=`date "+%F"`
ftp -n -v 15.207.173.6 << EOF
user Merchants hwMzZUhtRolr
pass
passive
mkdir ManualTest
cd ManaulTest
mkdir PowerAxisSRE_Test
cd PowerAxisSRE_Test
mkdir $todayis
cd $todayis
prompt
binary
hash
lcd /data/cronreport-payout/
put AXIS_ESCROW_NMP_PAYOUT_FILE_test.csv

bye
EOF
}

ftp_upload


#AXIS ESCROW MP PAYOUT FILE | Merchant


echo "
select (
                CASE
                        WHEN EXISTS(
                                SELECT 1
                                FROM bank_holidays
                                WHERE bank_holidays.date = curdate()
                                        OR DAYNAME(curdate()) = 'Sunday'
                                        OR (
                                                DAYNAME(curdate()) = 'Saturday'
                                                AND FLOOR((DAYOFMONTH(curdate()) + 6) / 7) IN (2, 4)
                                        )
                        ) then 'I' else 'N'
                end
        ) as 'Record Identifier',
        merchant_id as 'Beneficiary Code',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Execution Date',
        amount as 'Transaction amount',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Incoming Credit Date',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Transaction Intimation Date',
        batch_id as 'Additional Info 3',
        case
                when length(m.accHolderName) > 32 then trim(left(m.accHolderName, 32)) else m.accHolderName
        end as 'Additional Info 4',
        m.accHolderName as 'Account Name',
        m.enc_accNo as 'Account No.',
        m.enc_ifsc as 'IFSC Code',
        xtraInvestedAmount as 'XTRA INVESTMENT',
        'AXIS_ESCROW' as 'Bank type',
        batch_id as 'Original Batch',
        '' as 'On Demand Request Id',
        (
                select COALESCE(pal.amount_adjusted, 0.0)
                from payout_adjustment_ledger pal
                where ks.id = pal.settlement_id
                        and pal.settlement_type = 2
        ) as 'Loan Deduction'
from kotak_settlement ks,
        merchant m
where ks.merchant_id = m.mid
        and batch_id like concat('%', date_format(now(), '%Y%m%d'), '%')
        and ks.status = 'calculated' 
        and isXtraInvestmentMerchant = 0
        and (ismarketplace = 'y')
        and txn_date = date_format(now(), '%d/%m/%Y')
        and mid in (
                select mid
                from merchant_payout_config
                where power_access_file = 1
        );"| $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /data/cronreport-payout/AXIS_ESCROW_MP_MERCHANT_PAYOUT_FILE_test.csv


ftp_upload()
{
todayis=`date "+%F"`
ftp -n -v 15.207.173.6 << EOF
user Merchants hwMzZUhtRolr
pass
passive
mkdir ManualTest
cd ManaulTest
mkdir PowerAxisSRE_Test
cd PowerAxisSRE_Test
mkdir $todayis
cd $todayis
prompt
binary
hash
lcd /data/cronreport-payout/
put AXIS_ESCROW_MP_MERCHANT_PAYOUT_FILE_test.csv

bye
EOF
}

ftp_upload



#AXIS ESCROW MP PAYOUT FILE | SubMerchant


echo "
select (
                CASE
                        WHEN EXISTS(
                                SELECT 1
                                FROM bank_holidays
                                WHERE bank_holidays.date = curdate()
                                        OR DAYNAME(curdate()) = 'Sunday'
                                        OR (
                                                DAYNAME(curdate()) = 'Saturday'
                                                AND FLOOR((DAYOFMONTH(curdate()) + 6) / 7) IN (2, 4)
                                        )
                        ) then 'I' else 'N'
                end
        ) as 'Record Identifier',
        merchant_id as 'Beneficiary Code',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Execution Date',
        amount as 'Transaction amount',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Incoming Credit Date',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Transaction Intimation Date',
        batch_id as 'Additional Info 3',
        case
                when length(m.accHolderName) > 32 then trim(left(m.accHolderName, 32)) else m.accHolderName
        end as 'Additional Info 4',
        m.accHolderName as 'Account Name',
        m.enc_accNo as 'Account No.',
        m.enc_ifsc as 'IFSC Code',
        xtraInvestedAmount as 'XTRA INVESTMENT',
        'AXIS_ESCROW' as 'Bank type',
        batch_id as 'Original Batch',
        '' as 'On Demand Request Id',
        (
                select COALESCE(pal.amount_adjusted, 0.0)
                from payout_adjustment_ledger pal
                where ks.id = pal.settlement_id
                        and pal.settlement_type = 2
        ) as 'Loan Deduction'
from kotak_settlement ks,
        submerchant m
where ks.merchant_id = m.smid
        and batch_id like concat('%', date_format(now(), '%Y%m%d'), '%')
        and ks.status ='calculated'
        and isXtraInvestmentMerchant = 0
        and txn_date = date_format(now(), '%d/%m/%Y')
        and smid in (
                select mid
                from merchant_payout_config
                where power_access_file = 1
        );
"| $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /data/cronreport-payout/AXIS_ESCROW_MP_SUBMERCHANT_PAYOUT_FILE_test.csv

ftp_upload()
{
todayis=`date "+%F"`
ftp -n -v 15.207.173.6 << EOF
user Merchants hwMzZUhtRolr
pass
passive
mkdir ManualTest
cd ManaulTest
mkdir PowerAxisSRE_Test
cd PowerAxisSRE_Test
mkdir $todayis
cd $todayis
prompt
binary
hash
lcd /data/cronreport-payout/
put AXIS_ESCROW_MP_SUBMERCHANT_PAYOUT_FILE_test.csv

bye
EOF
}

ftp_upload




#AXIS NODAL MP PAYOUT FILE


echo "
select (
                CASE
                        WHEN EXISTS(
                                SELECT 1
                                FROM bank_holidays
                                WHERE bank_holidays.date = curdate()
                                        OR DAYNAME(curdate()) = 'Sunday'
                                        OR (
                                                DAYNAME(curdate()) = 'Saturday'
                                                AND FLOOR((DAYOFMONTH(curdate()) + 6) / 7) IN (2, 4)
                                        )
                        ) then 'I' else 'N'
                end
        ) as 'Record Identifier',
        merchant_id as 'Beneficiary Code',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Execution Date',
        amount as 'Transaction amount',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Incoming Credit Date',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Transaction Intimation Date',
        batch_id as 'Additional Info 3',
        case
                when length(m.accHolderName) > 32 then trim(left(m.accHolderName, 32)) else m.accHolderName
        end as 'Additional Info 4',
        m.accHolderName as 'Account Name',
        m.enc_accNo as 'Account No.',
        m.enc_ifsc as 'IFSC Code',
        xtraInvestedAmount as 'XTRA INVESTMENT',
        'AXIS_NODAL' as 'Bank type',
        batch_id as 'Original Batch',
        '' as 'On Demand Request Id',
        (
                select COALESCE(pal.amount_adjusted, 0.0)
                from payout_adjustment_ledger pal
                where sw.id = pal.settlement_id
                        and pal.settlement_type = 1
        ) as 'Loan Deduction'
from settlement_wapg sw,
        submerchant m
where sw.merchant_id = m.smid
        and sw.merchant_id not in (
                select mid
                from icici_payout_merchants
                where isPayoutEnabled = 1
        )
        and m.splittype = 0
        and m.enabledForCombinedPayout = 0
        and batch_id like concat('%', date_format(now(), '%Y%m%d'), '%')
        and sw.status ='calculated' 
        and sw.merchant_id in (
                select mid
                from merchant_payout_config
                where power_access_file = 1
        )
union all
select (
                CASE
                        WHEN EXISTS(
                                SELECT 1
                                FROM bank_holidays
                                WHERE bank_holidays.date = curdate()
                                        OR DAYNAME(curdate()) = 'Sunday'
                                        OR (
                                                DAYNAME(curdate()) = 'Saturday'
                                                AND FLOOR((DAYOFMONTH(curdate()) + 6) / 7) IN (2, 4)
                                        )
                        ) then 'I' else 'N'
                end
        ) as 'Record Identifier',
        merchant_id as 'Beneficiary Code',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Execution Date',
        amount as 'Transaction amount',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Incoming Credit Date',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Transaction Intimation Date',
        batch_id as 'Additional Info 3',
        case
                when length(m.accHolderName) > 32 then trim(left(m.accHolderName, 32)) else m.accHolderName
        end as 'Additional Info 4',
        m.accHolderName as 'Account Name',
        m.enc_accNo as 'Account No.',
        m.enc_ifsc as 'IFSC Code',
        xtraInvestedAmount as 'XTRA INVESTMENT',
        'AXIS_NODAL' as 'Bank type',
        batch_id as 'Original Batch',
        '' as 'On Demand Request Id',
        (
                select COALESCE(pal.amount_adjusted, 0.0)
                from payout_adjustment_ledger pal
                where sw.id = pal.settlement_id
                        and pal.settlement_type = 1
        ) as 'Loan Deduction'
from settlement_wapg sw,
        submerchant s,
        merchant m
where sw.merchant_id = s.smid
        and sw.merchant_id not in (
                select mid
                from icici_payout_merchants
                where isPayoutEnabled = 1
        )
        and s.splittype = 0
        and s.enabledForCombinedPayout = 1
        and batch_id like concat('%', date_format(now(), '%Y%m%d'), '%')
        and m.mid = s.parentmid
        and sw.status = 'calculated' 
        and sw.merchant_id in (
                select mid
                from merchant_payout_config
                where power_access_file = 1
        )
group by s.parentmid,
        batch_id
union all
select (
                CASE
                        WHEN EXISTS(
                                SELECT 1
                                FROM bank_holidays
                                WHERE bank_holidays.date = curdate()
                                        OR DAYNAME(curdate()) = 'Sunday'
                                        OR (
                                                DAYNAME(curdate()) = 'Saturday'
                                                AND FLOOR((DAYOFMONTH(curdate()) + 6) / 7) IN (2, 4)
                                        )
                        ) then 'I' else 'N'
                end
        ) as 'Record Identifier',
        merchant_id as 'Beneficiary Code',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Execution Date',
        amount as 'Transaction amount',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Incoming Credit Date',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Transaction Intimation Date',
        batch_id as 'Additional Info 3',
        case
                when length(m.accHolderName) > 32 then trim(left(m.accHolderName, 32)) else m.accHolderName
        end as 'Additional Info 4',
        m.accHolderName as 'Account Name',
        m.enc_accNo as 'Account No.',
        m.enc_ifsc as 'IFSC Code',
        xtraInvestedAmount as 'XTRA INVESTMENT',
        'AXIS_NODAL' as 'Bank type',
        batch_id as 'Original Batch',
        '' as 'On Demand Request Id',
        (
                select COALESCE(pal.amount_adjusted, 0.0)
                from payout_adjustment_ledger pal
                where sw.id = pal.settlement_id
                        and pal.settlement_type = 1
        ) as 'Loan Deduction'
from settlement_wapg sw,
        submerchant s,
        merchant m
where sw.merchant_id = m.mid
        and sw.merchant_id not in (
                select mid
                from icici_payout_merchants
                where isPayoutEnabled = 1
        )
        and s.splittype = 5
        and sw.batch_id like concat('%', date_format(now(), '%Y%m%d'), '%')
        and sw.status ='calculated'
        and m.mid = s.parentmid
        and ismarketplace = 'y'
        and sw.merchant_id in (
                select mid
                from merchant_payout_config
                where power_access_file = 1
        )
group by s.parentmid,
        batch_id;
"| $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /data/cronreport-payout/AXIS_NODAL_MP_PAYOUT_FILE_test.csv


ftp_upload()
{
todayis=`date "+%F"`
ftp -n -v 15.207.173.6 << EOF
user Merchants hwMzZUhtRolr
pass
passive
mkdir ManualTest
cd ManaulTest
mkdir PowerAxisSRE_Test
cd PowerAxisSRE_Test
mkdir $todayis
cd $todayis
prompt
binary
hash
lcd /data/cronreport-payout/
put AXIS_NODAL_MP_PAYOUT_FILE_test.csv

bye
EOF
}

ftp_upload



#AXIS NODAL NMP PAYOUT FILE


echo "
select (
                CASE
                        WHEN EXISTS(
                                SELECT 1
                                FROM bank_holidays
                                WHERE bank_holidays.date = curdate()
                                        OR DAYNAME(curdate()) = 'Sunday'
                                        OR (
                                                DAYNAME(curdate()) = 'Saturday'
                                                AND FLOOR((DAYOFMONTH(curdate()) + 6) / 7) IN (2, 4)
                                        )
                        ) then 'I' else 'N'
                end
        ) as 'Record Identifier',
        merchant_id as 'Beneficiary Code',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Execution Date',
        amount as 'Transaction amount',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Incoming Credit Date',
        DATE_FORMAT(STR_TO_DATE(txn_date, '%d/%m/%Y'), '%d-%b-%Y') as 'Transaction Intimation Date',
        batch_id as 'Additional Info 3',
        case
                when length(m.accHolderName) > 32 then trim(left(m.accHolderName, 32)) else m.accHolderName
        end as 'Additional Info 4',
        m.accHolderName as 'Account Name',
        m.enc_accNo as 'Account No.',
        m.enc_ifsc as 'IFSC Code',
        xtraInvestedAmount as 'XTRA INVESTMENT',
        'AXIS_NODAL' as 'Bank type',
        batch_id as 'Original Batch',
        '' as 'On Demand Request Id',
        (
                select COALESCE(pal.amount_adjusted, 0.0)
                from payout_adjustment_ledger pal
                where sw.id = pal.settlement_id
                        and pal.settlement_type = 1
        ) as 'Loan Deduction'
from settlement_wapg sw,
        merchant m
where sw.merchant_id = m.mid
        and sw.merchant_id not in (
                select mid
                from icici_payout_merchants
                where isPayoutEnabled = 1
        )
        and batch_id like concat('%', date_format(now(), '%Y%m%d'), '%') and sw.status = 'calculated' 
        and (
                ismarketplace is null
                or ismarketplace != 'y'
        )
        and sw.merchant_id in (
                select mid
                from merchant_payout_config
                where power_access_file = 1
        );"| $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /data/cronreport-payout/AXIS_NODAL_NMP_PAYOUT_FILE_test.csv


ftp_upload()
{
todayis=`date "+%F"`
ftp -n -v 15.207.173.6 << EOF
user Merchants hwMzZUhtRolr
pass
passive
mkdir ManualTest
cd ManaulTest
mkdir PowerAxisSRE_Test
cd PowerAxisSRE_Test
mkdir $todayis
cd $todayis
prompt
binary
hash
lcd /data/cronreport-payout/
put AXIS_NODAL_NMP_PAYOUT_FILE_test.csv

bye
EOF
}

ftp_upload


#AXIS NODAL NMP WORKING FILE

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
from wallet_as_pg_ledger wapg
        left join wallet_as_pg_ledger_metadata wapgm on (wapg.id = wapgm.parentid)
        left join pg_payment_order ppo on (wapgm.pgorderid = ppo.stamp)
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
        and (
                ismarketplace != 'y'
                or ismarketplace is null
        )
        and paymentType not in (2, 3, 4)
        and payoutbatchid in (
                select distinct(batch_id)
                from settlement_wapg
                where status = 'calculated' and created_at >= curdate()
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
from wallet_as_pg_ledger wapg
        left join wallet_as_pg_ledger_metadata wapgm on (wapg.id = wapgm.parentid)
        left join pg_payment_order ppo on (wapgm.pgorderid = ppo.stamp)
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
        and (
                ismarketplace != 'y'
                or ismarketplace is null
        )
        and paymentType not in (2, 3,4)
        and refundbatchid in (
                select distinct(batch_id)
                from settlement_wapg
                where status = 'calculated' and created_at >= curdate() 
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
order by 1;"| $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /data/cronreport-payout/AXIS_NODAL_NMP_WORKING_FILE_test.csv




}

Queries


ftp_upload()
{
todayis=`date "+%F"`
ftp -n -v 15.207.173.6 << EOF
user Merchants hwMzZUhtRolr
pass
passive
mkdir ManualTest
cd ManaulTest
mkdir PowerAxisSRE_Test
cd PowerAxisSRE_Test
mkdir $todayis
cd $todayis
prompt
binary
hash
lcd /data/cronreport-payout/
put AXIS_NODAL_NMP_WORKING_FILE_test.csv

bye
EOF
}

ftp_upload



Queries()
{



##AXIS ESCROW NMP WORKING FILE

echo "
select inner_query.* from (SELECT t.mid                   'MID',
       t.orderid               'ORDER_ID',
       t.id                    'TXN_ID',
       w.createdat             'TXN_Date',
       w.member_uid            'MEMBER_ID',
       w.merchantaliassupplied 'MERCHANT_NAME',
       w.txnamount             'TXN_AMOUNT',
       NULL                    AS 'RRN',
       w.totalfee              'FEE',
       w.totalservicetax       'Service_Tax',
       w.totalpayoutamt        'Payout_Amount',
       0                       AS 'Refund_Amount',
       w.payoutbatchid         'Payout_BATCH_ID',
       NULL                    AS 'Refund_BATCH_ID',
       Date(w.settlementdate)  'Settlement_date',
       -( -w.totalpayoutamt )  'Settlement_Amount',
       'CREDIT'                AS 'Settlement_Type',
       'INTEROP_WALLET_TXN'    AS TXN_TYPE,
       CASE
         WHEN collection_mode = '3' THEN 'Bijlipay_EDC'
         ELSE 'Mobikwik'
       END                     AS Collection_Mode,
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
         END )                 AS Payment_Instrument,
       mtmd.ext_ref_no         AS 'External_Refrence_Number',
       CASE
         WHEN t.memberuid = '85861954'
              AND paymenttype = 4 THEN 'ThirdPartyUPI'
         WHEN t.memberuid != '85861954'
              AND paymenttype = '4' THEN 'MobikwikUPI'
         ELSE NULL
       END                     UPI_MODE
FROM   txp t force index(updatedat)
       LEFT JOIN merchant_txp_meta_data mtmd
              ON ( t.id = mtmd.parent_id )
       LEFT JOIN wallet_as_pg_ledger w force index(idx_wallet_as_pg_ledger_updatedat)
              ON ( t.orderid = w.orderid
                   AND t.mid = w.mid
                   AND w.statecode IN ( 28, 38 ) )
       LEFT JOIN wallet_as_pg_ledger_metadata wm
              ON ( w.id = wm.parentid )
WHERE  w.settlementdate >= Date(Now())
       AND w.updatedat >= Date(Now())
       AND w.isnodalprocessed = 1
       AND paymenttype = 4
       AND partnertdr IN ( 1, 2 )
       AND t.mid NOT IN ( 'MBK5778' )
       AND t.statecode between 28 and 68 and t.updatedat>=date(now()) AND w.payoutbatchid IN (SELECT DISTINCT( batch_id )
                               FROM   kotak_settlement
                               WHERE status = 'calculated' and  created_at >= Curdate() )) inner_query inner join merchant_payout_config mpc on (inner_query.mid=mpc.mid and power_access_file=1 )
UNION ALL
select inner_query.* from (SELECT t.mid                   'MID',
       t.orderid               'ORDER_ID',
       t.id                    'TXN_ID',
       w.createdat             'TXN_Date',
       w.member_uid            'MEMBER_ID',
       w.merchantaliassupplied 'MERCHANT_NAME',
       w.txnamount             'TXN_AMOUNT',
       w.pgrefundrefid         AS 'RRN',
       0                       'FEE',
       0                       'Service_Tax',
       0                       'Payout_Amount',
       w.txnamount             AS 'Refund_Amount',
       NULL                    'Payout_BATCH_ID',
       w.refundbatchid         AS 'Refund_BATCH_ID',
       Date(w.settlementdate)  'Settlement_date',
       ( -w.txnamount )       'Settlement_Amount',
       'DEBIT'                 AS 'Settlement_Type',
       'INTEROP_WALLET_TXN'    AS TXN_TYPE,
       CASE
         WHEN collection_mode = '3' THEN 'Bijlipay_EDC'
         ELSE 'Mobikwik'
       END                     AS Collection_Mode,
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
         END )                 AS Payment_Instrument,
       mtmd.ext_ref_no         AS 'External_Refrence_Number',
       CASE
         WHEN t.memberuid = '85861954'
              AND paymenttype = 4 THEN 'ThirdPartyUPI'
         WHEN t.memberuid != '85861954'
              AND paymenttype = '4' THEN 'MobikwikUPI'
         ELSE NULL
       END                     UPI_MODE
FROM   txp t force index(updatedat)
       LEFT JOIN merchant_txp_meta_data mtmd
              ON ( t.id = mtmd.parent_id )
       LEFT JOIN wallet_as_pg_ledger w force index(idx_wallet_as_pg_ledger_updatedat)
              ON ( t.orderid = w.orderid
                   AND t.mid = w.mid
                   AND w.statecode > 200 )
       LEFT JOIN wallet_as_pg_ledger_metadata wm
              ON ( w.id = wm.parentid )
WHERE  w.settlementdate >= Date(Now())
       AND w.updatedat >= Date(Now())
       AND w.isnodalprocessed = 1
       AND paymenttype = 4
       AND partnertdr IN ( 1, 2 )
       AND t.mid NOT IN ( 'MBK5778' )
	   AND t.statecode between 28 and 68 and t.updatedat>date(now())  AND w.refundbatchid IN (SELECT DISTINCT( batch_id )
                               FROM   kotak_settlement
                               WHERE status = 'calculated' and created_at >= Curdate())) inner_query inner join merchant_payout_config mpc on (inner_query.mid=mpc.mid and power_access_file=1 )

UNION ALL
select inner_query.* from (SELECT t.mid                   'MID',
       t.orderid               'ORDER_ID',
       t.id                    'TXN_ID',
       t.createdat             'TXN_Date',
       t.memberuid              'MEMBER_ID',
       t.merchantaliassupplied 'MERCHANT_NAME',
       t.txnamount             'TXN_AMOUNT',
       NULL                    AS 'RRN',
       fee                     'FEE',
       servicetax              'Service_Tax',
       t.payoutamt             'Payout_Amount',
       0                       AS 'Refund_Amount',
       t.payoutbatchid         'Payout_BATCH_ID',
       NULL                    AS 'Refund_BATCH_ID',
       Date(c.createdat)       'Settlement_date',
       -( -t.payoutamt )       'Settlement_Amount',
       'CREDIT'                AS 'Settlement_Type',
       'ZIP_WALLET_Component'  AS TXN_TYPE,
       CASE
         WHEN collection_mode = '3' THEN 'Bijlipay_EDC'
         ELSE 'Mobikwik'
       END                     AS Collection_Mode,
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
         END )                 AS Payment_Instrument,
       mtmd.ext_ref_no         AS 'External_Refrence_Number',
       CASE
         WHEN t.memberuid = '85861954'
              AND paymenttype = 4 THEN 'ThirdPartyUPI'
         WHEN t.memberuid != '85861954'
              AND paymenttype = '4' THEN 'MobikwikUPI'
         ELSE NULL
       END                     UPI_MODE
FROM   txp t force index(updatedat)
       LEFT JOIN merchant_txp_meta_data mtmd
              ON ( t.id = mtmd.parent_id )
       LEFT JOIN txc c force index(idx_txc_createdat)
              ON ( t.id = c.parentid )
       LEFT JOIN wallet_as_pg_ledger w force index(idx_wallet_as_pg_ledger_updatedat)
              ON ( t.orderid = w.orderid
                   AND t.mid = w.mid )
       LEFT JOIN wallet_as_pg_ledger_metadata wm
              ON ( w.id = wm.parentid )
WHERE  c.createdat >= Date(Now())
       AND c.statecode = 35
       AND paymenttype = 2
       AND w.statecode IN ( 28, 38 )
       AND partnertdr = 1
       AND t.mid NOT IN ( 'MBK5778' )
       AND t.payoutbatchid IN (SELECT DISTINCT( batch_id )
                               FROM   kotak_settlement
                               WHERE  status = 'calculated' and created_at >= Curdate())
        and t.statecode between 28 and 68
		and w.updatedat>date(now())
        and t.updatedat>date(now())	) inner_query inner join merchant_payout_config mpc on (inner_query.mid=mpc.mid and power_access_file=1 )
UNION ALL
select inner_query.* from (SELECT t.mid                   'MID',
       t.orderid               'ORDER_ID',
       t.id                    'TXN_ID',
       t.createdat             'TXN_Date',
       t.memberuid              'MEMBER_ID',
       t.merchantaliassupplied 'Merchant_Name',
       t.txnamount             'TXN_AMOUNT',
       NULL                    AS 'RRN',
       fee                     'FEE',
       servicetax              'Service_Tax',
       t.payoutamt             'Payout_Amount',
       0                       AS 'Refund_Amount',
       t.payoutbatchid         'Payout_BATCH_ID',
       NULL                    AS 'Refund_BATCH_ID',
       Date(c.createdat)       'Settlement_date',
       -( -t.payoutamt )       'Settlement_Amount',
       'CREDIT'                AS 'Settlement_Type',
       'GLOBAL_MID'            AS TXN_TYPE,
       CASE
         WHEN collection_mode = '3' THEN 'Bijlipay_EDC'
         ELSE 'Mobikwik'
       END                     AS Collection_Mode,
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
         END )                 AS Payment_Instrument,
       mtmd.ext_ref_no         AS 'External_Refrence_Number',
       CASE
         WHEN t.memberuid = '85861954'
              AND paymenttype = 4 THEN 'ThirdPartyUPI'
         WHEN t.memberuid != '85861954'
              AND paymenttype = '4' THEN 'MobikwikUPI'
         ELSE NULL
       END                     UPI_MODE
FROM   txp t force index(updatedat)
       LEFT JOIN merchant_txp_meta_data mtmd
              ON ( t.id = mtmd.parent_id )
       LEFT JOIN txc c force index(idx_txc_createdat)
              ON ( t.id = c.parentid )
       LEFT JOIN wallet_as_pg_ledger w force index(idx_wallet_as_pg_ledger_updatedat)
              ON ( t.orderid = w.orderid
                   AND t.mid = w.mid )
       LEFT JOIN wallet_as_pg_ledger_metadata wm
              ON ( w.id = wm.parentid )
WHERE  c.createdat >= Date(Now())
       AND c.statecode = 35
       AND paymenttype != 2
       AND w.statecode IN ( 28, 38 )
       AND partnertdr = 1
       AND t.mid NOT IN ( 'MBK5778' )
	   and t.updatedat>date(now())
	   and w.updatedat>date(now())
       AND t.payoutbatchid NOT IN (SELECT DISTINCT( batch_id )
                                   FROM   merchant_settlement_request
                                   WHERE  created_at >= Curdate())
       AND t.payoutbatchid IN (SELECT DISTINCT( batch_id )
                               FROM   kotak_settlement
                               WHERE  status = 'calculated' and created_at >= Curdate())
       ) inner_query inner join merchant_payout_config mpc on (inner_query.mid=mpc.mid and power_access_file=1 )
UNION ALL
select inner_query.* from (SELECT t.mid                   'MID',
       t.orderid               'ORDER_ID',
       t.id                    'TXN_ID',
       t.createdat             'TXN_Date',
       t.memberuid              'MEMBER_ID',
       t.merchantaliassupplied 'Merchant_Name',
       t.txnamount             'TXN_AMOUNT',
       NULL                    AS 'RRN',
       fee                     'FEE',
       servicetax              'Service_Tax',
       t.payoutamt             'Payout_Amount',
       0                       AS 'Refund_Amount',
       t.payoutbatchid         'Payout_BATCH_ID',
       NULL                    AS 'Refund_BATCH_ID',
       Date(c.createdat)       'Settlement_date',
       -( -t.payoutamt )       'Settlement_Amount',
       'CREDIT'                AS 'Settlement_Type',
       'PURE_WALLET'           AS TXN_TYPE,
       CASE
         WHEN collection_mode = '3' THEN 'Bijlipay_EDC'
         ELSE 'Mobikwik'
       END                     AS Collection_Mode,
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
         END )                 AS Payment_Instrument,
       mtmd.ext_ref_no         AS 'External_Refrence_Number',
       NULL                    UPI_MODE
FROM   txp t force index(updatedat)
       LEFT JOIN merchant_txp_meta_data mtmd
              ON ( t.id = mtmd.parent_id )
       LEFT JOIN txc c force index(idx_txc_createdat)
              ON ( t.id = c.parentid )
WHERE  c.createdat >= Date(Now())
       AND c.statecode = 35
       AND partnertdr IS NULL
       AND t.mid NOT IN ( 'MBK5778' )
	   and t.updatedat>date(now())
       AND t.payoutbatchid IN (SELECT DISTINCT( batch_id )
                               FROM   kotak_settlement
                               WHERE status = 'calculated' and created_at >= Curdate())
                               ) inner_query inner join merchant_payout_config mpc on (inner_query.mid=mpc.mid and power_access_file=1 )

UNION ALL
select inner_query.* from (SELECT t.mid                   'MID',
       t.orderid               'OrderId',
       t.id                    'TXN_ID',
       t.createdat             'TXN_Date',
       t.memberuid              'Member_Id',
       t.merchantaliassupplied 'MERCHANT_NAME',
       t.txnamount             'TXN_Amount',
       c.id                    'RRN',
       0                       'FEE',
       0                       'Service_Tax',
       0                       'Payout_Amount',
       -( CASE
            WHEN c.statecode IN ( 66, 46 ) THEN amount
            ELSE t.txnamount
          END )                'Refund_Amount',
       NULL                    'Payout_BATCH_ID',
       CASE
         WHEN c.statecode IN ( 44, 45 ) THEN t.refundbatchid
         ELSE c.rrn
       END                     'Refund_BATCH_ID',
       Date(c.createdat)       'Settlement_Date',
       -( CASE
            WHEN c.statecode IN ( 66, 46 ) THEN amount
            ELSE t.txnamount
          END )                'Settlement_Amount',
       'DEBIT'                 AS 'Settlement_Type',
       'Pure_Wallet'           AS TXN_TYPE,
       CASE
         WHEN collection_mode = '3' THEN 'Bijlipay_EDC'
         ELSE 'Mobikwik'
       END                     AS Collection_Mode,
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
         END )                 AS Payment_Instrument,
       mtmd.ext_ref_no         AS 'External_Refrence_Number',
       NULL                    UPI_MODE
FROM   txp t force index(updatedat)
       LEFT JOIN merchant_txp_meta_data mtmd
              ON ( t.id = mtmd.parent_id )
       LEFT JOIN txc c force index(idx_txc_createdat)
              ON ( t.id = c.parentid )
WHERE  c.createdat >= Date(Now())
       AND c.statecode IN ( 44, 45, 66, 46 )
       AND partnertdr IS NULL
       AND t.mid NOT IN ( 'MBK5778' )
	   and t.updatedat>date(now())
       AND t.refundbatchid IN (SELECT DISTINCT( batch_id )
                               FROM   kotak_settlement
                               WHERE status = 'calculated' and created_at >= Curdate())
       ) inner_query inner join merchant_payout_config mpc on (inner_query.mid=mpc.mid and power_access_file=1 )
UNION ALL
select inner_query.* from (SELECT t.mid                   'MID',
       t.orderid               'OrderId',
       t.id                    'TXN_ID',
       t.createdat             'TXN_Date',
       t.memberuid              'Member_Id',
       t.merchantaliassupplied 'MERCHANT_NAME',
       t.txnamount             'TXN_Amount',
       c.id                    'RRN',
       0                       'FEE',
       0                       'Service_Tax',
       0                       'Payout_Amount',
       -( CASE
            WHEN c.statecode IN ( 66, 46 ) THEN amount
            ELSE t.txnamount
          END )                'Refund_Amount',
       NULL                    'Payout_BATCH_ID',
       CASE
         WHEN c.statecode IN ( 44, 45 ) THEN t.refundbatchid
         ELSE c.rrn
       END                     'Refund_BATCH_ID',
       Date(c.createdat)       'Settlement_Date',
       -( CASE
            WHEN c.statecode IN ( 66, 46 ) THEN amount
            ELSE t.txnamount
          END )                'Settlement_Amount',
       'DEBIT'                 AS 'Settlement_Type',
       'ZIP_WALLET_Component'  AS TXN_TYPE,
       CASE
         WHEN collection_mode = '3' THEN 'Bijlipay_EDC'
         ELSE 'Mobikwik'
       END                     AS Collection_Mode,
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
         END )                 AS Payment_Instrument,
       mtmd.ext_ref_no         AS 'External_Refrence_Number',
       CASE
         WHEN t.memberuid = '85861954'
              AND paymenttype = 4 THEN 'ThirdPartyUPI'
         WHEN t.memberuid != '85861954'
              AND paymenttype = '4' THEN 'MobikwikUPI'
         ELSE NULL
       END                     UPI_MODE
FROM   txp t force index(updatedat)
       LEFT JOIN merchant_txp_meta_data mtmd
              ON ( t.id = mtmd.parent_id )
       LEFT JOIN txc c force index(idx_txc_createdat)
              ON ( t.id = c.parentid )
       LEFT JOIN wallet_as_pg_ledger w force index(idx_wallet_as_pg_ledger_updatedat)
              ON ( t.orderid = w.orderid
                   AND t.mid = w.mid )
       LEFT JOIN wallet_as_pg_ledger_metadata wm
              ON ( w.id = wm.parentid )
WHERE  c.createdat >= Date(Now())
       AND c.statecode IN ( 44, 45, 66, 46 )
       AND partnertdr = 1
       AND paymenttype = 2
       AND w.statecode IN ( 28, 38 )
       AND t.mid NOT IN ( 'MBK5778' )
	   and t.updatedat>date(now())
	   and w.updatedat>date(now())
       AND t.refundbatchid IN (SELECT DISTINCT( batch_id )
                               FROM   kotak_settlement
                               WHERE status = 'calculated' and created_at >= Curdate())
                               ) inner_query inner join merchant_payout_config mpc on (inner_query.mid=mpc.mid and power_access_file=1 )
UNION ALL
select inner_query.* from (SELECT t.mid                   'MID',
       t.orderid               'OrderId',
       t.id                    'TXN_ID',
       t.createdat             'TXN_Date',
       t.memberuid              'Member_Id',
       t.merchantaliassupplied 'MERCHANT_NAME',
       t.txnamount             'TXN_Amount',
       c.id                    'RRN',
       0                       'FEE',
       0                       'Service_Tax',
       0                       'Payout_Amount',
       -( CASE
            WHEN c.statecode IN ( 66, 46 ) THEN amount
            ELSE t.txnamount
          END )                'Refund_Amount',
       NULL                    'Payout_BATCH_ID',
       CASE
         WHEN c.statecode IN ( 44, 45 ) THEN t.refundbatchid
         ELSE c.rrn
       END                     'Refund_BATCH_ID',
       Date(c.createdat)       'Settlement_Date',
       -( CASE
            WHEN c.statecode IN ( 66, 46 ) THEN amount
            ELSE t.txnamount
          END )                'Settlement_Amount',
       'DEBIT'                 AS 'Settlement_Type',
       'GLOBAL_MID'            AS TXN_TYPE,
       CASE
         WHEN collection_mode = '3' THEN 'Bijlipay_EDC'
         ELSE 'Mobikwik'
       END                     AS Collection_Mode,
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
         END )                 AS Payment_Instrument,
       mtmd.ext_ref_no         AS 'External_Refrence_Number',
       CASE
         WHEN t.memberuid = '85861954'
              AND paymenttype = 4 THEN 'ThirdPartyUPI'
         WHEN t.memberuid != '85861954'
              AND paymenttype = '4' THEN 'MobikwikUPI'
         ELSE NULL
       END                     UPI_MODE
FROM   txp t force index(updatedat)
       LEFT JOIN merchant_txp_meta_data mtmd
              ON ( t.id = mtmd.parent_id )
       LEFT JOIN txc c force index(idx_txc_createdat)
              ON ( t.id = c.parentid )
       LEFT JOIN wallet_as_pg_ledger w force index(idx_wallet_as_pg_ledger_updatedat)
              ON ( t.orderid = w.orderid
                   AND t.mid = w.mid )
       LEFT JOIN wallet_as_pg_ledger_metadata wm
              ON ( w.id = wm.parentid )
WHERE  c.createdat >= Date(Now())
       AND c.statecode IN ( 44, 45, 66, 46 )
       AND partnertdr = 1
       AND paymenttype != 2
       AND w.statecode IN ( 28, 38 )
       AND t.mid NOT IN ( 'MBK5778' )
	   and t.updatedat>date(now())
	   and w.updatedat>date(now())
HAVING refund_batch_id NOT IN (SELECT DISTINCT( batch_id )
                               FROM   merchant_settlement_request
                               WHERE  created_at >= Curdate())
       AND refund_batch_id IN (SELECT DISTINCT( batch_id )
                               FROM   kotak_settlement
                               WHERE status = 'calculated' and created_at >= Curdate()) ) inner_query inner join merchant_payout_config mpc on (inner_query.mid=mpc.mid and power_access_file=1 );
"| $MYSQL --login-path=mobinewcronmaster_RDS01 -D $DB | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /data/cronreport-payout/AXIS_ESCROW_NMP_WORKING_FILE_test.csv


}

Queries



ftp_upload()
{
todayis=`date "+%F"`
ftp -n -v 15.207.173.6 << EOF
user Merchants hwMzZUhtRolr
pass
passive
mkdir ManualTest
cd ManualTest
mkdir PowerAxisSRE_Test
cd PowerAxisSRE_Test
mkdir $todayis
cd $todayis
prompt
binary
hash
lcd /data/cronreport-payout/
put AXIS_ESCROW_NMP_WORKING_FILE_test.csv

bye
EOF
}

ftp_upload

bash -xf /var/scripts/check_emptyfiles_New.sh >> /tmp/cronlogs/check_emptyfiles.log

