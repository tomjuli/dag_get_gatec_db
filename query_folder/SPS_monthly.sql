-- SET @periode = "2026-05" COLLATE utf8mb4_unicode_ci;
WITH payroll_raw_cte AS (
SELECT 
	periode,
	id,
    member_id,
    employee_id,
    name as employee_name,
    addin_join_date as join_date,
    addin_termination_date as termination_date,
    addin_position_name as job_position,
    addin_upah as upah,
    back_pay as rapel,
    addin_grade_name as grade,
    addin_tunjangan_acting as acting_allowance,
    addin_total_hk as total_hk,
    addin_total_overtime_duration as total_overtime_hours,
    addin_employment_statuses as employee_type,
    addin_department_name as department,
    work_location_name as afdeling,
    addin_premi_traksi as premi_traksi,
    addin_premi_tetap as premi_tetap,
    addin_premi_teknis as premi_teknis,
    addin_premi_lainnya as premi_lainnya,
    addin_premi_supervisi as premi_supervisi,
    addin_premi_support as premi_support,
    (addin_premi_traksi + addin_premi_tetap + addin_premi_teknis +addin_premi_lainnya + addin_premi_supervisi + addin_premi_support + monthly_incentive) as total_premi,
    addin_overtime as overtime, -- rupiah,
    addin_rupiah_mangkir as potongan_mangkir,
    addin_total_allowance as total_allowance,
    addin_pph21_deduction as pph21,
    addin_loan_deduction as pinjaman,
    overpayment as lebih_bayar,
    total_payable as thp
        -- (addin_upah + (addin_premi_traksi + addin_premi_tetap + addin_premi_teknis +addin_premi_lainnya + addin_premi_supervisi + addin_premi_support + monthly_incentive))
FROM payroll_data_items pdi WHERE deleted_at IS NULL
AND periode=DATE_FORMAT(CAST(DATE_FORMAT(NOW() ,'%%Y-%%m-01') as DATE) - INTERVAL 1 DAY, '%%Y-%%m')
),
 bpjs_reports_cte AS(
 SELECT
    br.periode,
    brd.payroll_data_item_id,
    brd.member_id,
    SUM(CASE WHEN brd.bpjs_def = 'JHT'
        THEN brd.corporate_contribution ELSE 0 END) AS jht_company,
    SUM(CASE WHEN brd.bpjs_def = 'JHT'
        THEN brd.employee_contribution ELSE 0 END) AS jht_employee,
    SUM(CASE WHEN brd.bpjs_def = 'JKM'
        THEN brd.corporate_contribution ELSE 0 END) AS jkm_company,
    SUM(CASE WHEN brd.bpjs_def = 'JKM'
        THEN brd.employee_contribution ELSE 0 END) AS jkm_employee,
    SUM(CASE WHEN brd.bpjs_def = 'JP'
        THEN brd.corporate_contribution ELSE 0 END) AS jp_company,
    SUM(CASE WHEN brd.bpjs_def = 'JP'
        THEN brd.employee_contribution ELSE 0 END) AS jp_employee,
    SUM(CASE WHEN brd.bpjs_def = 'KS'
        THEN brd.corporate_contribution ELSE 0 END) AS ks_company,
    SUM(CASE WHEN brd.bpjs_def = 'KS'
        THEN brd.employee_contribution ELSE 0 END) AS ks_employee,
         -- JKK (JKK1, JKK2, JKK3)
    SUM(CASE WHEN brd.bpjs_def IN ('JKK1', 'JKK2', 'JKK3', 'JKK4')
        THEN brd.corporate_contribution ELSE 0 END) AS jkk_company,
    SUM(CASE WHEN brd.bpjs_def IN ('JKK1', 'JKK2', 'JKK3', 'JKK4')
        THEN brd.employee_contribution ELSE 0 END) AS jkk_employee
FROM bpjs_report_details brd
JOIN bpjs_reports br
    ON br.id = brd.bpjs_report_id
WHERE brd.deleted_at IS NULL
  AND br.periode = @periode
  AND brd.deleted_at IS NULL
GROUP BY
    br.periode,
    brd.payroll_data_item_id,
    brd.member_id
),
bpjs_report_summary as(
        SELECT member_id,
                periode, 
                payroll_data_item_id,
        jkk_company,
        jkm_company, 
        ks_company,
                -- penambah
                (jkk_company + jkm_company) as total_jkk,
                ks_company as total_bpjsks,
                -- pengurang
                (jkk_company + jkm_company + jht_employee + jp_employee) as potongan_jamsostek,
                (ks_company + ks_employee) as potongan_bpjs_kesehatan
  FROM bpjs_reports_cte
)
select 
	prc.periode,
        department,
    afdeling,
        employee_id,
    employee_name,
    employee_type,
    grade,
    join_date,
    termination_date,
    job_position,
    total_hk,
    total_overtime_hours,
    upah,
    rapel,
    acting_allowance,
   --  premi_traksi,
    -- premi_tetap,
    -- premi_teknis,
    -- premi_lainnya,
    -- premi_supervisi,
    -- premi_support,
    total_premi,
    total_allowance,
    overtime,
    total_jkk,
    total_bpjsks,
    (upah + total_premi + overtime + rapel + jkk_company + jkm_company + ks_company  + total_allowance) total_income,
    potongan_mangkir,
    potongan_jamsostek,
    potongan_bpjs_kesehatan,
    (pinjaman + lebih_bayar) as potongan_pinjaman,
     pph21,
    (pinjaman + lebih_bayar) + potongan_bpjs_kesehatan + potongan_jamsostek + pph21 + potongan_mangkir as total_potongan,
    thp
     FROM payroll_raw_cte prc
     LEFT JOIN bpjs_report_summary brs ON prc.member_id=brs.member_id
                AND prc.id=brs.payroll_data_item_id
