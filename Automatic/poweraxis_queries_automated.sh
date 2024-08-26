source ~/.bashrc
Queries()
{

#This file contains :
#AXIS ESCROW MP PAYOUT FILE | Merchant
#AXIS ESCROW MP PAYOUT FILE | SubMerchant
#AXIS NODAL MP PAYOUT FILE
#AXIS NODAL MP WORKING FILE
#AXIS NODAL NMP PAYOUT FILE
#AXIS NODAL NMP WORKING FILE
#AXIS ESCROW MP WORKING FILE
##AXIS ESCROW NMP WORKING FILE
#AXIS ESCROW NMP PAYOUT FILE


echo "select 'txp' as Table_name,
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
from mobinew.wallet_as_pg_ledger_metadata;

"| draco_da | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /apps/cron/Adarsh/Draco_crons/PowerAxis/Out/Tables_timestamp_data_test.csv


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
        and batch_id like concat('%', date_format(now(), '%Y%m%d'), '%') and ks.status = 'automated_success' OR ks.status = 'automated_failure' OR ks.status = 'automated_pending' --checked
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
"| draco_da | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /apps/cron/Adarsh/Draco_crons/PowerAxis/Out/AXIS_ESCROW_NMP_PAYOUT_FILE_test.csv


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
        and isXtraInvestmentMerchant = 0 and ks.status = 'automated_success' OR ks.status = 'automated_failure' OR ks.status = 'automated_pending' --changed
        and (ismarketplace = 'y')
        and txn_date = date_format(now(), '%d/%m/%Y')
        and mid in (
                select mid
                from merchant_payout_config
                where power_access_file = 1
        );"|draco_da | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /apps/cron/Adarsh/Draco_crons/PowerAxis/Out/AXIS_ESCROW_MP_MERCHANT_PAYOUT_FILE_test.csv



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
        and batch_id like concat('%', date_format(now(), '%Y%m%d'), '%') and ks.status = 'automated_success' OR ks.status = 'automated_failure' OR ks.status = 'automated_pending' --checked
        and isXtraInvestmentMerchant = 0
        and txn_date = date_format(now(), '%d/%m/%Y')
        and smid in (
                select mid
                from merchant_payout_config
                where power_access_file = 1
        );
"|draco_da | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /apps/cron/Adarsh/Draco_crons/PowerAxis/Out/AXIS_ESCROW_MP_SUBMERCHANT_PAYOUT_FILE_test.csv



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
        and sw.status = 'automated_success' OR sw.status = 'automated_failure' OR sw.status = 'automated_pending'
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
        and sw.status = 'automated_success' OR sw.status = 'automated_failure' OR sw.status = 'automated_pending'
        and batch_id like concat('%', date_format(now(), '%Y%m%d'), '%')
        and m.mid = s.parentmid
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
        and sw.status = 'automated_success' OR sw.status = 'automated_failure' OR sw.status = 'automated_pending'
        and sw.batch_id like concat('%', date_format(now(), '%Y%m%d'), '%')
        and m.mid = s.parentmid
        and ismarketplace = 'y'
        and sw.merchant_id in (
                select mid
                from merchant_payout_config
                where power_access_file = 1
        )
group by s.parentmid,
        batch_id;
"|draco_da | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /apps/cron/Adarsh/Draco_crons/PowerAxis/Out/AXIS_NODAL_MP_PAYOUT_FILE_test.csv



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
                when wapgm.paymenttype = 1 then 'UPI'
                when wapgm.paymenttype = 0 then 'WAPG' else 'ZIP'
        end aS 'TxnMode'
from wallet_as_pg_ledger wapg force index(idx_wallet_as_pg_ledger_updatedat)
        left join wallet_as_pg_ledger_metadata wapgm force index(PRIMARY) on (wapg.id = wapgm.parentid)
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
                where status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending' and created_at >= curdate()
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
                when wapgm.paymenttype = 1 then 'UPI'
                when wapgm.paymenttype = 0 then 'WAPG' else 'ZIP'
        end aS 'TxnMode'
from wallet_as_pg_ledger wapg force index(idx_wallet_as_pg_ledger_updatedat)
        left join wallet_as_pg_ledger_metadata wapgm force index(PRIMARY) on (wapg.id = wapgm.parentid)
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
                where status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending' and created_at >= curdate()
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
"|draco_da | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /apps/cron/Adarsh/Draco_crons/PowerAxis/Out/AXIS_NODAL_MP_WORKING_FILE_test.csv


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
where sw.merchant_id = m.mid and sw.status = 'automated_success' OR sw.status = 'automated_failure' OR sw.status = 'automated_pending'
        and sw.merchant_id not in (
                select mid
                from icici_payout_merchants
                where isPayoutEnabled = 1
        )
        and batch_id like concat('%', date_format(now(), '%Y%m%d'), '%')
        and (
                ismarketplace is null
                or ismarketplace != 'y'
        )
        and sw.merchant_id in (
                select mid
                from merchant_payout_config
                where power_access_file = 1
        );"|draco_da | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /apps/cron/Adarsh/Draco_crons/PowerAxis/Out/AXIS_NODAL_NMP_PAYOUT_FILE_test.csv


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
        left join pg_Payment_order ppo on (wapgm.pgorderid = ppo.stamp)
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
                where status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending' and created_at >= curdate() --changed
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
        left join pg_Payment_order ppo on (wapgm.pgorderid = ppo.stamp)
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
                where status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending' and created_at >= curdate()
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
order by 1;"| draco_da | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /apps/cron/Adarsh/Draco_crons/PowerAxis/Out/AXIS_NODAL_NMP_WORKING_FILE_test.csv



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
      and status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending'
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
      and status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending'
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
      and status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending'
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
      and status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending'
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
      and status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending'
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
      and status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending') and t.statecode between 28 and 68;
"|draco_da| sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /apps/cron/Adarsh/Draco_crons/PowerAxis/Out/AXIS_ESCORW_MP_WORKING_FILE_test.csv

}

Queries


ftp_upload()
{
todayis=`date "+%F"`
ftp -n -v 15.207.173.6 << EOF
user Merchants hwMzZUhtRolr
pass
passive
mkdir Automated_Test
cd Automated_Test
mkdir PowerAxis_Automated
cd Poweraxis_Automated
mkdir PowerAxisAN_Automated
cd PowerAxisAN_Automated
mkdir $todayis
cd $todayis
prompt
binary
hash
lcd /apps/cron/Adarsh/Draco_crons/PowerAxis/Out/
mput *
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
                               WHERE  created_at >= Curdate() AND status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending')) inner_query inner join merchant_payout_config mpc on (inner_query.mid=mpc.mid and power_access_file=1 )
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
                               WHERE status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending' and created_at >= Curdate())) inner_query inner join merchant_payout_config mpc on (inner_query.mid=mpc.mid and power_access_file=1 )

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
                               WHERE  status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending' and created_at >= Curdate())
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
                               WHERE  status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending' and created_at >= Curdate()) --changed
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
                               WHERE  status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending' and created_at >= Curdate()) --changed
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
                               WHERE  status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending' and created_at >= Curdate())
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
                               WHERE  status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending' and created_at >= Curdate())
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
                               WHERE  status = 'automated_success' OR status = 'automated_failure' OR status = 'automated_pending' and created_at >= Curdate()) ) inner_query inner join merchant_payout_config mpc on (inner_query.mid=mpc.mid and power_access_file=1 );
"|draco_da | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > /apps/cron/Adarsh/Draco_crons/PowerAxis/Out/AXIS_ESCROW_NMP_WORKING_FILE_test.csv


}

Queries



ftp_upload()
{
todayis=`date "+%F"`
ftp -n -v 15.207.173.6 << EOF
user Merchants hwMzZUhtRolr
pass
passive
mkdir Automated_Test
cd Automated_Test
mkdir PowerAxis_Automated
cd Poweraxis_Automated
mkdir PowerAxisAN_Automated
cd PowerAxisAN_Automated
mkdir $todayis
cd $todayis
prompt
binary
hash
lcd /apps/cron/Adarsh/Draco_crons/PowerAxis/Out/
mput *
bye
EOF
}

ftp_upload
