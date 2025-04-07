{{ config(alias="microdados", schema="br_datahackers_state_data") }}
select
    safe_cast(p0 as string) p0,
    safe_cast(p1_a as int64) p1_a,
    safe_cast(p1_a_1 as string) p1_a_1,
    safe_cast(p1_b as string) p1_b,
    safe_cast(p1_c as string) p1_c,
    safe_cast(p1_d as string) p1_d,
    safe_cast(p1_e as string) p1_e,
    safe_cast(p1_f as string) p1_f,
    safe_cast(p1_g as boolean) p1_g,
    safe_cast(p1_i as string) p1_i,
    safe_cast(p1_i_1 as string) p1_i_1,
    safe_cast(p1_i_2 as string) p1_i_2,
    safe_cast(p1_j as boolean) p1_j,
    safe_cast(p1_k as string) p1_k,
    safe_cast(p1_l as string) p1_l,
    safe_cast(p1_m as string) p1_m,
    safe_cast(p2_a as string) p2_a,
    safe_cast(p2_b as string) p2_b,
    safe_cast(p2_c as string) p2_c,
    safe_cast(p2_d as boolean) p2_d,
    safe_cast(p2_e as string) p2_e,
    safe_cast(p2_f as string) p2_f,
    safe_cast(p2_g as string) p2_g,
    safe_cast(p2_h as string) p2_h,
    safe_cast(p2_i as string) p2_i,
    safe_cast(p2_j as string) p2_j,
    safe_cast(p2_k as boolean) p2_k,
    safe_cast(p2_l as string) p2_l,
    safe_cast(p2_l_1 as int64) p2_l_1,
    safe_cast(p2_l_2 as int64) p2_l_2,
    safe_cast(p2_l_3 as int64) p2_l_3,
    safe_cast(p2_l_4 as int64) p2_l_4,
    safe_cast(p2_l_5 as int64) p2_l_5,
    safe_cast(p2_l_6 as int64) p2_l_6,
    safe_cast(p2_l_7 as int64) p2_l_7,
    safe_cast(p2_m as string) p2_m,
    safe_cast(p2_n as string) p2_n,
    safe_cast(p2_o as string) p2_o,
    safe_cast(p2_o_1 as int64) p2_o_1,
    safe_cast(p2_o_2 as int64) p2_o_2,
    safe_cast(p2_o_3 as int64) p2_o_3,
    safe_cast(p2_o_4 as int64) p2_o_4,
    safe_cast(p2_o_5 as int64) p2_o_5,
    safe_cast(p2_o_6 as int64) p2_o_6,
    safe_cast(p2_o_7 as int64) p2_o_7,
    safe_cast(p2_o_8 as int64) p2_o_8,
    safe_cast(p2_o_9 as int64) p2_o_9,
    safe_cast(p2_o_10 as int64) p2_o_10,
    safe_cast(p2_p as string) p2_p,
    safe_cast(p2_q as string) p2_q,
    safe_cast(p2_r as string) p2_r,
    safe_cast(p2_s as string) p2_s,
    safe_cast(p3_a as string) p3_a,
    safe_cast(p3_b as string) p3_b,
    safe_cast(p3_b_1 as int64) p3_b_1,
    safe_cast(p3_b_2 as int64) p3_b_2,
    safe_cast(p3_b_3 as int64) p3_b_3,
    safe_cast(p3_b_4 as int64) p3_b_4,
    safe_cast(p3_b_5 as int64) p3_b_5,
    safe_cast(p3_b_6 as int64) p3_b_6,
    safe_cast(p3_b_7 as int64) p3_b_7,
    safe_cast(p3_b_8 as int64) p3_b_8,
    safe_cast(p3_b_9 as int64) p3_b_9,
    safe_cast(p3_c as string) p3_c,
    safe_cast(p3_c_1 as int64) p3_c_1,
    safe_cast(p3_c_2 as int64) p3_c_2,
    safe_cast(p3_c_3 as int64) p3_c_3,
    safe_cast(p3_c_4 as int64) p3_c_4,
    safe_cast(p3_c_5 as int64) p3_c_5,
    safe_cast(p3_c_6 as int64) p3_c_6,
    safe_cast(p3_c_7 as int64) p3_c_7,
    safe_cast(p3_c_8 as int64) p3_c_8,
    safe_cast(p3_c_9 as int64) p3_c_9,
    safe_cast(p3_c_10 as int64) p3_c_10,
    safe_cast(p3_c_11 as int64) p3_c_11,
    safe_cast(p3_d as string) p3_d,
    safe_cast(p3_d_1 as int64) p3_d_1,
    safe_cast(p3_d_2 as int64) p3_d_2,
    safe_cast(p3_d_3 as int64) p3_d_3,
    safe_cast(p3_d_4 as int64) p3_d_4,
    safe_cast(p3_d_5 as int64) p3_d_5,
    safe_cast(p3_d_6 as int64) p3_d_6,
    safe_cast(p3_d_7 as int64) p3_d_7,
    safe_cast(p3_d_8 as int64) p3_d_8,
    safe_cast(p3_d_9 as int64) p3_d_9,
    safe_cast(p3_d_10 as int64) p3_d_10,
    safe_cast(p3_d_11 as int64) p3_d_11,
    safe_cast(p3_d_12 as int64) p3_d_12,
    safe_cast(p3_d_13 as int64) p3_d_13,
    safe_cast(p3_d_14 as int64) p3_d_14,
    safe_cast(p4_a as string) p4_a,
    safe_cast(p4_a_1 as string) p4_a_1,
    safe_cast(p4_b as string) p4_b,
    safe_cast(p4_b_1 as int64) p4_b_1,
    safe_cast(p4_b_2 as int64) p4_b_2,
    safe_cast(p4_b_3 as int64) p4_b_3,
    safe_cast(p4_b_4 as int64) p4_b_4,
    safe_cast(p4_b_5 as int64) p4_b_5,
    safe_cast(p4_b_6 as int64) p4_b_6,
    safe_cast(p4_b_7 as int64) p4_b_7,
    safe_cast(p4_b_8 as int64) p4_b_8,
    safe_cast(p4_c as string) p4_c,
    safe_cast(p4_c_1 as int64) p4_c_1,
    safe_cast(p4_c_2 as int64) p4_c_2,
    safe_cast(p4_c_3 as int64) p4_c_3,
    safe_cast(p4_c_4 as int64) p4_c_4,
    safe_cast(p4_c_5 as int64) p4_c_5,
    safe_cast(p4_c_6 as int64) p4_c_6,
    safe_cast(p4_c_7 as int64) p4_c_7,
    safe_cast(p4_c_8 as int64) p4_c_8,
    safe_cast(p4_d as string) p4_d,
    safe_cast(p4_d_1 as int64) p4_d_1,
    safe_cast(p4_d_2 as int64) p4_d_2,
    safe_cast(p4_d_3 as int64) p4_d_3,
    safe_cast(p4_d_4 as int64) p4_d_4,
    safe_cast(p4_d_5 as int64) p4_d_5,
    safe_cast(p4_d_6 as int64) p4_d_6,
    safe_cast(p4_d_7 as int64) p4_d_7,
    safe_cast(p4_d_8 as int64) p4_d_8,
    safe_cast(p4_d_9 as int64) p4_d_9,
    safe_cast(p4_d_10 as int64) p4_d_10,
    safe_cast(p4_d_11 as int64) p4_d_11,
    safe_cast(p4_d_12 as int64) p4_d_12,
    safe_cast(p4_d_13 as int64) p4_d_13,
    safe_cast(p4_d_14 as int64) p4_d_14,
    safe_cast(p4_e as string) p4_e,
    safe_cast(p4_f as string) p4_f,
    safe_cast(p4_g as string) p4_g,
    safe_cast(p4_g_1 as int64) p4_g_1,
    safe_cast(p4_g_2 as int64) p4_g_2,
    safe_cast(p4_g_3 as int64) p4_g_3,
    safe_cast(p4_f_4 as int64) p4_f_4,
    safe_cast(p4_f_5 as int64) p4_f_5,
    safe_cast(p4_f_6 as int64) p4_f_6,
    safe_cast(p4_f_7 as int64) p4_f_7,
    safe_cast(p4_f_8 as int64) p4_f_8,
    safe_cast(p4_f_9 as int64) p4_f_9,
    safe_cast(p4_f_10 as int64) p4_f_10,
    safe_cast(p4_f_11 as int64) p4_f_11,
    safe_cast(p4_f_12 as int64) p4_f_12,
    safe_cast(p4_f_13 as int64) p4_f_13,
    safe_cast(p4_f_14 as int64) p4_f_14,
    safe_cast(p4_f_15 as int64) p4_f_15,
    safe_cast(p4_f_16 as int64) p4_f_16,
    safe_cast(p4_f_17 as int64) p4_f_17,
    safe_cast(p4_f_18 as int64) p4_f_18,
    safe_cast(p4_f_19 as int64) p4_f_19,
    safe_cast(p4_f_20 as int64) p4_f_20,
    safe_cast(p4_f_21 as int64) p4_f_21,
    safe_cast(p4_f_22 as int64) p4_f_22,
    safe_cast(p4_f_23 as int64) p4_f_23,
    safe_cast(p4_f_24 as int64) p4_f_24,
    safe_cast(p4_f_25 as int64) p4_f_25,
    safe_cast(p4_f_26 as int64) p4_f_26,
    safe_cast(p4_f_27 as int64) p4_f_27,
    safe_cast(p4_f_28 as int64) p4_f_28,
    safe_cast(p4_f_29 as int64) p4_f_29,
    safe_cast(p4_f_30 as int64) p4_f_30,
    safe_cast(p4_f_31 as int64) p4_f_31,
    safe_cast(p4_f_32 as int64) p4_f_32,
    safe_cast(p4_f_33 as int64) p4_f_33,
    safe_cast(p4_g__1 as string) p4_g__1,
    safe_cast(p4_h as string) p4_h,
    safe_cast(p4_h_1 as int64) p4_h_1,
    safe_cast(p4_h_2 as int64) p4_h_2,
    safe_cast(p4_h_3 as int64) p4_h_3,
    safe_cast(p4_i as string) p4_i,
    safe_cast(p4_i_1 as int64) p4_i_1,
    safe_cast(p4_i_2 as int64) p4_i_2,
    safe_cast(p4_i_3 as int64) p4_i_3,
    safe_cast(p4_i_4 as int64) p4_i_4,
    safe_cast(p4_i_5 as int64) p4_i_5,
    safe_cast(p4_i_6 as int64) p4_i_6,
    safe_cast(p4_i_7 as int64) p4_i_7,
    safe_cast(p4_i_8 as int64) p4_i_8,
    safe_cast(p4_i_9 as int64) p4_i_9,
    safe_cast(p4_i_10 as int64) p4_i_10,
    safe_cast(p4_i_11 as int64) p4_i_11,
    safe_cast(p4_i_12 as int64) p4_i_12,
    safe_cast(p4_i_13 as int64) p4_i_13,
    safe_cast(p4_i_14 as int64) p4_i_14,
    safe_cast(p4_i_15 as int64) p4_i_15,
    safe_cast(p4_i_16 as int64) p4_i_16,
    safe_cast(p4_i_17 as int64) p4_i_17,
    safe_cast(p4_i_18 as int64) p4_i_18,
    safe_cast(p4_i_19 as int64) p4_i_19,
    safe_cast(p4_i_20 as int64) p4_i_20,
    safe_cast(p4_i_21 as int64) p4_i_21,
    safe_cast(p4_i_22 as int64) p4_i_22,
    safe_cast(p4_i_23 as int64) p4_i_23,
    safe_cast(p5_a as string) p5_a,
    safe_cast(p5_b as string) p5_b,
    safe_cast(p5_c as string) p5_c,
    safe_cast(p5_d as string) p5_d,
    safe_cast(p6_a as string) p6_a,
    safe_cast(p6_a_1 as int64) p6_a_1,
    safe_cast(p6_a_2 as int64) p6_a_2,
    safe_cast(p6_a_3 as int64) p6_a_3,
    safe_cast(p6_a_4 as int64) p6_a_4,
    safe_cast(p6_a_5 as int64) p6_a_5,
    safe_cast(p6_a_6 as int64) p6_a_6,
    safe_cast(p6_a_7 as int64) p6_a_7,
    safe_cast(p6_a_8 as int64) p6_a_8,
    safe_cast(p6_a_9 as int64) p6_a_9,
    safe_cast(p6_b as string) p6_b,
    safe_cast(p6_b_1 as int64) p6_b_1,
    safe_cast(p6_b_2 as int64) p6_b_2,
    safe_cast(p6_b_3 as int64) p6_b_3,
    safe_cast(p6_b_4 as int64) p6_b_4,
    safe_cast(p6_b_5 as int64) p6_b_5,
    safe_cast(p6_b_6 as int64) p6_b_6,
    safe_cast(p6_b_7 as int64) p6_b_7,
    safe_cast(p6_b_8 as int64) p6_b_8,
    safe_cast(p6_b_9 as int64) p6_b_9,
    safe_cast(p6_b_10 as int64) p6_b_10,
    safe_cast(p6_b_11 as int64) p6_b_11,
    safe_cast(p6_b_12 as int64) p6_b_12,
    safe_cast(p6_b_13 as int64) p6_b_13,
    safe_cast(p6_b_14 as int64) p6_b_14,
    safe_cast(p6_b_15 as int64) p6_b_15,
    safe_cast(p6_b_16 as int64) p6_b_16,
    safe_cast(p6_b_17 as int64) p6_b_17,
    safe_cast(p6_b_18 as int64) p6_b_18,
    safe_cast(p6_b_19 as int64) p6_b_19,
    safe_cast(p6_b_19__1 as int64) p6_b_19__1,
    safe_cast(p6_c as boolean) p6_c,
    safe_cast(p6_d as string) p6_d,
    safe_cast(p6_e as boolean) p6_e,
    safe_cast(p6_f as string) p6_f,
    safe_cast(p6_g as string) p6_g,
    safe_cast(p6_g_1 as int64) p6_g_1,
    safe_cast(p6_g_2 as int64) p6_g_2,
    safe_cast(p6_g_3 as int64) p6_g_3,
    safe_cast(p6_g_4 as int64) p6_g_4,
    safe_cast(p6_g_5 as int64) p6_g_5,
    safe_cast(p6_g_6 as int64) p6_g_6,
    safe_cast(p6_g_7 as int64) p6_g_7,
    safe_cast(p6_g_8 as int64) p6_g_8,
    safe_cast(p6_g_9 as int64) p6_g_9,
    safe_cast(p6_g_10 as int64) p6_g_10,
    safe_cast(p6_g_11 as int64) p6_g_11,
    safe_cast(p6_g_l as int64) p6_g_l,
    safe_cast(p6_g_m as int64) p6_g_m,
    safe_cast(p6_h as string) p6_h,
    safe_cast(p6_h_1 as int64) p6_h_1,
    safe_cast(p6_h_2 as int64) p6_h_2,
    safe_cast(p6_h_3 as int64) p6_h_3,
    safe_cast(p6_h_4 as int64) p6_h_4,
    safe_cast(p6_h_5 as int64) p6_h_5,
    safe_cast(p6_h_6 as int64) p6_h_6,
    safe_cast(p6_h_7 as int64) p6_h_7,
    safe_cast(p6_h_8 as int64) p6_h_8,
    safe_cast(p6_h_9 as int64) p6_h_9,
    safe_cast(p7_1 as string) p7_1,
    safe_cast(p7_a_1 as int64) p7_a_1,
    safe_cast(p7_a_2 as int64) p7_a_2,
    safe_cast(p7_a_3 as int64) p7_a_3,
    safe_cast(p7_a_4 as int64) p7_a_4,
    safe_cast(p7_a_5 as int64) p7_a_5,
    safe_cast(p7_a_6 as int64) p7_a_6,
    safe_cast(p7_a_7 as int64) p7_a_7,
    safe_cast(p7_a_8 as int64) p7_a_8,
    safe_cast(p7_a_9 as int64) p7_a_9,
    safe_cast(p7_a_10 as int64) p7_a_10,
    safe_cast(p7_b as string) p7_b,
    safe_cast(p7_b_1 as int64) p7_b_1,
    safe_cast(p7_b_2 as int64) p7_b_2,
    safe_cast(p7_b_3 as int64) p7_b_3,
    safe_cast(p7_b_4 as int64) p7_b_4,
    safe_cast(p7_b_5 as int64) p7_b_5,
    safe_cast(p7_b_6 as int64) p7_b_6,
    safe_cast(p7_b_7 as int64) p7_b_7,
    safe_cast(p7_b_8 as int64) p7_b_8,
    safe_cast(p7_b_9 as int64) p7_b_9,
    safe_cast(p7_b_10 as int64) p7_b_10,
    safe_cast(p7_b_11 as int64) p7_b_11,
    safe_cast(p7_b_12 as int64) p7_b_12,
    safe_cast(p7_b_13 as int64) p7_b_13,
    safe_cast(p7_b_14 as int64) p7_b_14,
    safe_cast(p7_b_15 as int64) p7_b_15,
    safe_cast(p7_b_16 as int64) p7_b_16,
    safe_cast(p7_b_17 as int64) p7_b_17,
    safe_cast(p7_b_18 as int64) p7_b_18,
    safe_cast(p7_b_19 as int64) p7_b_19,
    safe_cast(p7_b_20 as int64) p7_b_20,
    safe_cast(p7_c as string) p7_c,
    safe_cast(p7_c_1 as int64) p7_c_1,
    safe_cast(p7_c_2 as int64) p7_c_2,
    safe_cast(p7_c_3 as int64) p7_c_3,
    safe_cast(p7_c_4 as int64) p7_c_4,
    safe_cast(p7_c_5 as int64) p7_c_5,
    safe_cast(p7_c_6 as int64) p7_c_6,
    safe_cast(p7_d as string) p7_d,
    safe_cast(p7_d_1 as int64) p7_d_1,
    safe_cast(p7_d_2 as int64) p7_d_2,
    safe_cast(p7_d_3 as int64) p7_d_3,
    safe_cast(p7_d_4 as int64) p7_d_4,
    safe_cast(p7_d_5 as int64) p7_d_5,
    safe_cast(p7_d_6 as int64) p7_d_6,
    safe_cast(p7_d_7 as int64) p7_d_7,
    safe_cast(p7_d_8 as int64) p7_d_8,
    safe_cast(p7_d_9 as int64) p7_d_9,
    safe_cast(p7_d_10 as int64) p7_d_10,
    safe_cast(p8_a as string) p8_a,
    safe_cast(p8_a_1 as int64) p8_a_1,
    safe_cast(p8_a_2 as int64) p8_a_2,
    safe_cast(p8_a_3 as int64) p8_a_3,
    safe_cast(p8_a_4 as int64) p8_a_4,
    safe_cast(p8_a_5 as int64) p8_a_5,
    safe_cast(p8_a_6 as int64) p8_a_6,
    safe_cast(p8_a_7 as int64) p8_a_7,
    safe_cast(p8_a_8 as int64) p8_a_8,
    safe_cast(p8_a_9 as int64) p8_a_9,
    safe_cast(p8_a_10 as int64) p8_a_10,
    safe_cast(p8_a_11 as int64) p8_a_11,
    safe_cast(p8_b as string) p8_b,
    safe_cast(p8_b_1 as int64) p8_b_1,
    safe_cast(p8_b_2 as int64) p8_b_2,
    safe_cast(p8_b_3 as int64) p8_b_3,
    safe_cast(p8_b_4 as int64) p8_b_4,
    safe_cast(p8_b_5 as int64) p8_b_5,
    safe_cast(p8_b_6 as int64) p8_b_6,
    safe_cast(p8_b_7 as int64) p8_b_7,
    safe_cast(p8_b_8 as int64) p8_b_8,
    safe_cast(p8_b_9 as int64) p8_b_9,
    safe_cast(p8_b_10 as int64) p8_b_10,
    safe_cast(p8_b_11 as int64) p8_b_11,
    safe_cast(p8_b_l as int64) p8_b_l,
    safe_cast(p8_b_m as int64) p8_b_m,
    safe_cast(p8_3 as string) p8_3,
    safe_cast(p8_c_1 as int64) p8_c_1,
    safe_cast(p8_c_2 as int64) p8_c_2,
    safe_cast(p8_c_3 as int64) p8_c_3,
    safe_cast(p8_c_4 as int64) p8_c_4,
    safe_cast(p8_c_5 as int64) p8_c_5,
    safe_cast(p8_c_6 as int64) p8_c_6,
    safe_cast(p8_c_7 as int64) p8_c_7,
    safe_cast(p8_c_8 as int64) p8_c_8,
    safe_cast(p8_c_9 as int64) p8_c_9,
    safe_cast(p8_c_10 as int64) p8_c_10,
    safe_cast(p8_c_11 as int64) p8_c_11,
    safe_cast(p8_d as string) p8_d,
    safe_cast(p8_d_1 as int64) p8_d_1,
    safe_cast(p8_d_2 as int64) p8_d_2,
    safe_cast(p8_d_3 as int64) p8_d_3,
    safe_cast(p8_d_4 as int64) p8_d_4,
    safe_cast(p8_d_5 as int64) p8_d_5,
    safe_cast(p8_d_6 as int64) p8_d_6,
    safe_cast(p8_d_7 as int64) p8_d_7,
    safe_cast(p8_d_8 as int64) p8_d_8,
    safe_cast(p8_d_9 as int64) p8_d_9,
    safe_cast(p8_d_10 as int64) p8_d_10,
    safe_cast(p8_d_11 as int64) p8_d_11,
from {{ set_datalake_project("br_datahackers_state_data_staging.microdados") }} as t
