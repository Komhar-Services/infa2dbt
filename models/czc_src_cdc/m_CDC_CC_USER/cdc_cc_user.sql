{{ config(
    alias="cdc_cc_user",
    materialized="incremental",
    post_hook=[{"sql": "UPDATE {{ var('dbownerprst') }}.dbaall.CDC_CC_USER  AS TGT
                SET LTST_REC_IND = 'N'
                WHERE TGT.ID IN (SELECT ID FROM {{ var('dbownerprst') }}.dbaall.CDC_CC_USER AS SRC
                                  WHERE TGT.ID = SRC.ID 
                                  AND SRC.BAT_ID = {{ var('batch_id') }} 
                                  AND SRC.LTST_REC_IND = 'Y') 
                AND TGT.BAT_ID <> {{ var('batch_id') }} 
                AND TGT.LTST_REC_IND = 'Y';"}]
    ) 
}}
with
    r_ulkp_wpcd_eff_fm_tistmp as (select * from {{ ref("r_ulkp_wpcd_eff_fm_tistmp") }}),
    cc_user as (select * from {{ source("dbaall", "cc_user") }}),
    sq_cc_user as (
        with
            latest_id_record as (
                select urs.id, max(urs.snapshot_date) max_snpsht_tistmp
                from cc_user urs
                where urs.snapshot_date > '{{ var("czc_startdate") }}'
                group by urs.id
            )
        select
            ltrim(rtrim(externaluser)) as externaluser,
            validationlevel,
            retired,
            organizationid,
            vacationstatus,
            ltrim(rtrim(publicid)) as publicid,
            createuserid,
            credentialid,
            usr.id,
            beanversion,
            timezone,
            ltrim(rtrim(department)) department,
            updateuserid,
            policytype,
            null::NUMBER(38,0) as integerext,  -- modified as part of CR#5301
            usersettingsid,
            loadcommandid,
            contactid,
            authorityprofileid,
            updatetime,
            ltrim(rtrim(authoritycomments)) as authoritycomments,
            experiencelevel,
            ltrim(rtrim(basiccomments)) as basiccomments,
            quickclaim,
            createtime,
            losstype,
            systemusertype,
            language,
            newlyassignedactivities,
            ltrim(rtrim(jobtitle)) as jobtitle,
            offsetstatsupdatetime,
            z_specialhandlertype,
            z_accountid,
            claimorgid,
            row_number() over (order by null) rn
        from cc_user usr
        inner join
            latest_id_record lir
            on lir.id = usr.id
            and lir.max_snpsht_tistmp = usr.snapshot_date
        where usr.snapshot_date > '{{ var("czc_startdate") }}'
    )    ,
    r_exp_audit_cols_czc_cdc as (
        select
            externaluser as in_dummy,
            {{ var("pop_info_id") }} as out_pop_info_id,
            {{ var("pop_info_id") }} as out_updt_pop_info_id,
            {{ var("batch_id") }} as out_bat_id,
            -- *INF*: :LKP.r_ULKP_WPCD_EFF_FM_TISTMP(1)
            r_ulkp_wpcd_eff_fm_tistmp_1.mntry_end_prcg_tistmp as out_eff_fm_tistmp,
            -- *INF*: TO_DATE('2999-12-31-00.00.00.000000', 'YYYY-MM-DD HH24:MI:SS NS')
            to_timestamp(
                '2999-12-31 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'
            ) as out_eff_to_tistmp,
            'Y' as out_ltst_rec_ind,
            rn
        from sq_cc_user
        left join
            r_ulkp_wpcd_eff_fm_tistmp r_ulkp_wpcd_eff_fm_tistmp_1
            on r_ulkp_wpcd_eff_fm_tistmp_1.dummy = 1

    )
select
    sq_cc_user.externaluser as externaluser,
    sq_cc_user.validationlevel as validationlevel,
    sq_cc_user.retired as retired,
    sq_cc_user.organizationid as organizationid,
    sq_cc_user.vacationstatus as vacationstatus,
    sq_cc_user.publicid as publicid,
    sq_cc_user.createuserid as createuserid,
    sq_cc_user.credentialid as credentialid,
    sq_cc_user.id as id,
    sq_cc_user.beanversion as beanversion,
    sq_cc_user.timezone as timezone,
    sq_cc_user.department as department,
    sq_cc_user.updateuserid as updateuserid,
    sq_cc_user.policytype as policytype,
    sq_cc_user.integerext as integerext,
    sq_cc_user.usersettingsid as usersettingsid,
    sq_cc_user.loadcommandid as loadcommandid,
    sq_cc_user.contactid as contactid,
    sq_cc_user.authorityprofileid as authorityprofileid,
    sq_cc_user.updatetime as updatetime,
    sq_cc_user.authoritycomments as authoritycomments,
    sq_cc_user.experiencelevel as experiencelevel,
    sq_cc_user.basiccomments as basiccomments,
    sq_cc_user.quickclaim as quickclaim,
    sq_cc_user.createtime as createtime,
    sq_cc_user.losstype as losstype,
    sq_cc_user.systemusertype as systemusertype,
    sq_cc_user.language as language,
    sq_cc_user.newlyassignedactivities as newlyassignedactivities,
    sq_cc_user.jobtitle as jobtitle,
    sq_cc_user.offsetstatsupdatetime as offsetstatsupdatetime,
    sq_cc_user.z_specialhandlertype as z_specialhandlertype,
    sq_cc_user.z_accountid as z_accountid,
    sq_cc_user.claimorgid as claimorgid,
    r_exp_audit_cols_czc_cdc.out_pop_info_id as pop_info_id,
    r_exp_audit_cols_czc_cdc.out_updt_pop_info_id as updt_pop_info_id,
    r_exp_audit_cols_czc_cdc.out_bat_id as bat_id,
    r_exp_audit_cols_czc_cdc.out_eff_fm_tistmp as eff_fm_tistmp,
    r_exp_audit_cols_czc_cdc.out_eff_to_tistmp as eff_to_tistmp,
    r_exp_audit_cols_czc_cdc.out_ltst_rec_ind as ltst_rec_ind
from sq_cc_user
inner join r_exp_audit_cols_czc_cdc using (rn)
