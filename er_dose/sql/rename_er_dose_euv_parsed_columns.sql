do $$
declare
    partition_table regclass;

    function column_exists(target_table regclass, target_column text) returns boolean as $fn$
    begin
        return exists (
            select 1
            from pg_attribute
            where attrelid = target_table
              and attname = target_column
              and attnum > 0
              and not attisdropped
        );
    end;
    $fn$ language plpgsql;

    procedure rename_column_if_exists(target_table regclass, old_name text, new_name text) as $proc$
    begin
        if column_exists(target_table, old_name) and not column_exists(target_table, new_name) then
            execute format('alter table %s rename column %I to %I', target_table, old_name, new_name);
        end if;
    end;
    $proc$ language plpgsql;
begin
    if to_regclass('prism_common.er_dose_euv_parsed') is null then
        raise notice 'table prism_common.er_dose_euv_parsed does not exist; skip rename';
        return;
    end if;

    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'source_exposure_id', 'exposure_id');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'exposure id', 'exposure_id');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'source_code_occur_time', 'time');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'source_file_name', 'dose_error_detected_in_file');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'dose error detected in file', 'dose_error_detected_in_file');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'root_cause_message', 'root_cause');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'root cause', 'root_cause');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'exposure length', 'exposure_length');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'duty cycle', 'duty_cycle');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'min. dose error', 'min_dose_error');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'max. dose error', 'max_dose_error');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'on drop euv energy', 'on_drop_euv_energy');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'on drop pp energy', 'on_drop_pp_energy');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'on drop mp energy', 'on_drop_mp_energy');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'on_drop_pp_dlgc1', 'on_drop_pp_dlgc_1');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'on drop pp dlgc=1', 'on_drop_pp_dlgc_1');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'on_drop_mp_dlgc1', 'on_drop_mp_dlgc_1');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'on drop mp dlgc=1', 'on_drop_mp_dlgc_1');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'bi-cell y 3sigma', 'bi_cell_y_3sigma');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'fdsc y error', 'fdsc_y_error');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'fdsc y 3sigma', 'fdsc_y_3sigma');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'max. cross. interval', 'max_cross_interval');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'xint 3sigma', 'xint_3sigma');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'euv 3sigma', 'euv_3sigma');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'pulses_euv<0.6dt_tot', 'pulses_euv_0_6dt_tot');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'pulses_euv_lt_0_6dt_tot', 'pulses_euv_0_6dt_tot');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'fed pulses', 'fed_pulses');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'l2dx maxce', 'l2dx_maxce');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'l2dy maxce', 'l2dy_maxce');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'sensitivity at l2dx maxce', 'sensitivity_at_l2dx_maxce');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'sensitivity at l2dy maxce', 'sensitivity_at_l2dy_maxce');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'dose margin', 'dose_margin');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'l2dx qc etdc 3sigma', 'l2dx_qc_etdc_3sigma');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'l2dx qc etdc median', 'l2dx_qc_etdc_median');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'l2dy qc etdc 3sigma', 'l2dy_qc_etdc_3sigma');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'l2dy qc etdc median', 'l2dy_qc_etdc_median');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'rbdy peak frequency hf', 'rbdy_peak_frequency_hf');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'rbdy peak frequency lf', 'rbdy_peak_frequency_lf');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'rbdy peak frequency mf', 'rbdy_peak_frequency_mf');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'rbdy peak power hf', 'rbdy_peak_power_hf');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'rbdy qc etdc 3sigma', 'rbdy_qc_etdc_3sigma');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'rbdy total power lf', 'rbdy_total_power_lf');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'rbdy total power mf', 'rbdy_total_power_mf');
    call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'software version', 'software_version');

    for partition_table in
        select inhrelid::regclass
        from pg_inherits
        where inhparent = 'prism_common.er_dose_euv_parsed'::regclass
    loop
        call rename_column_if_exists(partition_table, 'source_exposure_id', 'exposure_id');
        call rename_column_if_exists(partition_table, 'exposure id', 'exposure_id');
        call rename_column_if_exists(partition_table, 'source_code_occur_time', 'time');
        call rename_column_if_exists(partition_table, 'source_file_name', 'dose_error_detected_in_file');
        call rename_column_if_exists(partition_table, 'dose error detected in file', 'dose_error_detected_in_file');
        call rename_column_if_exists(partition_table, 'root_cause_message', 'root_cause');
        call rename_column_if_exists(partition_table, 'root cause', 'root_cause');
        call rename_column_if_exists(partition_table, 'exposure length', 'exposure_length');
        call rename_column_if_exists(partition_table, 'duty cycle', 'duty_cycle');
        call rename_column_if_exists(partition_table, 'min. dose error', 'min_dose_error');
        call rename_column_if_exists(partition_table, 'max. dose error', 'max_dose_error');
        call rename_column_if_exists(partition_table, 'on drop euv energy', 'on_drop_euv_energy');
        call rename_column_if_exists(partition_table, 'on drop pp energy', 'on_drop_pp_energy');
        call rename_column_if_exists(partition_table, 'on drop mp energy', 'on_drop_mp_energy');
        call rename_column_if_exists(partition_table, 'on_drop_pp_dlgc1', 'on_drop_pp_dlgc_1');
        call rename_column_if_exists(partition_table, 'on drop pp dlgc=1', 'on_drop_pp_dlgc_1');
        call rename_column_if_exists(partition_table, 'on_drop_mp_dlgc1', 'on_drop_mp_dlgc_1');
        call rename_column_if_exists(partition_table, 'on drop mp dlgc=1', 'on_drop_mp_dlgc_1');
        call rename_column_if_exists(partition_table, 'bi-cell y 3sigma', 'bi_cell_y_3sigma');
        call rename_column_if_exists(partition_table, 'fdsc y error', 'fdsc_y_error');
        call rename_column_if_exists(partition_table, 'fdsc y 3sigma', 'fdsc_y_3sigma');
        call rename_column_if_exists(partition_table, 'max. cross. interval', 'max_cross_interval');
        call rename_column_if_exists(partition_table, 'xint 3sigma', 'xint_3sigma');
        call rename_column_if_exists(partition_table, 'euv 3sigma', 'euv_3sigma');
        call rename_column_if_exists(partition_table, 'pulses_euv<0.6dt_tot', 'pulses_euv_0_6dt_tot');
        call rename_column_if_exists(partition_table, 'pulses_euv_lt_0_6dt_tot', 'pulses_euv_0_6dt_tot');
        call rename_column_if_exists(partition_table, 'fed pulses', 'fed_pulses');
        call rename_column_if_exists(partition_table, 'l2dx maxce', 'l2dx_maxce');
        call rename_column_if_exists(partition_table, 'l2dy maxce', 'l2dy_maxce');
        call rename_column_if_exists(partition_table, 'sensitivity at l2dx maxce', 'sensitivity_at_l2dx_maxce');
        call rename_column_if_exists(partition_table, 'sensitivity at l2dy maxce', 'sensitivity_at_l2dy_maxce');
        call rename_column_if_exists(partition_table, 'dose margin', 'dose_margin');
        call rename_column_if_exists(partition_table, 'l2dx qc etdc 3sigma', 'l2dx_qc_etdc_3sigma');
        call rename_column_if_exists(partition_table, 'l2dx qc etdc median', 'l2dx_qc_etdc_median');
        call rename_column_if_exists(partition_table, 'l2dy qc etdc 3sigma', 'l2dy_qc_etdc_3sigma');
        call rename_column_if_exists(partition_table, 'l2dy qc etdc median', 'l2dy_qc_etdc_median');
        call rename_column_if_exists(partition_table, 'rbdy peak frequency hf', 'rbdy_peak_frequency_hf');
        call rename_column_if_exists(partition_table, 'rbdy peak frequency lf', 'rbdy_peak_frequency_lf');
        call rename_column_if_exists(partition_table, 'rbdy peak frequency mf', 'rbdy_peak_frequency_mf');
        call rename_column_if_exists(partition_table, 'rbdy peak power hf', 'rbdy_peak_power_hf');
        call rename_column_if_exists(partition_table, 'rbdy qc etdc 3sigma', 'rbdy_qc_etdc_3sigma');
        call rename_column_if_exists(partition_table, 'rbdy total power lf', 'rbdy_total_power_lf');
        call rename_column_if_exists(partition_table, 'rbdy total power mf', 'rbdy_total_power_mf');
        call rename_column_if_exists(partition_table, 'software version', 'software_version');
    end loop;
end;
$$;
