package sql_server_lineage

import (
	"fmt"
	"io"
	"os"
	"slices"
	"testing"
)

func TestLexerAndParserValidStrings(t *testing.T) {
	tests := map[string]map[string]map[string][]string{
		"../test_data/generate_missing_key_rubrik_monthly.txt": {
			"prefix1_shows.schema.missing_key_rubrik_monthly": {
				"schema.generate_missing_key_rubrik_monthly": []string{
					"prefix1_shows.schema.farm_all_key_rubrik",
					"prefix1_prefix2.schema.prefix2_date",
					"prefix1_prefix2.schema.prefix2_bazar",
				},
			},
		},
		"../test_data/get_all_ttt_exchanges.txt": {
			"schema.ttt_exchanges_viber_show": {
				"schema.get_all_ttt_exchanges": []string{
					"prefix1_prefix2.schema.prefix2_fr_ttt",
				},
			},
			"schema.ttt_exchanges_dividends_show": {
				"schema.get_all_ttt_exchanges": []string{
					"prefix1_prefix2.schema.prefix2_fr_ttt",
					"prefix1_prefix2.schema.prefix2_ttt_dividendted_exchanges",
				},
			},
		},
		"../test_data/get_all_exchanges_merge.txt": {
			"prefix1_shows.schema.all_exchanges_merge_v1": {
				"schema.get_all_exchanges_merge": []string{
					"prefix1_prefix2.schema.prefix2_fc_merged_output_brzo",
					"prefix1_prefix2.schema.prefix2_fc_merged_output",
					"prefix1_prefix2.schema.prefix2_bazar",
				},
			},
		},
		"../test_data/get_fr_sending_model_shows_sea_20230511.txt": {
			"prefix1_shows.schema.sending_model_done_show_sea_20230511": {
				"schema.get_fr_sending_model_shows_sea_20230511": []string{
					"prefix1_prefix2.schema.prefix2_ext_sending_sea_20230511",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin_freezed",
					"prefix1_prefix2.schema.prefix2_ext_ppin",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin",
				},
			},
		},
		"../test_data/get_fr_sending_model_shows_dev.txt": {
			"prefix1_shows.schema.sending_model_done_show": {
				"schema.get_fr_sending_model_shows_dev": []string{
					"prefix1_prefix2.schema.prefix2_ext_sending",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin_freezed",
					"prefix1_prefix2.schema.prefix2_ext_ppin",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin",
				},
			},
		},
		"../test_data/get_fr_sending_model_shows_test_20230425.txt": {
			"prefix1_shows.schema.sending_model_done_show_sea_20230425": {
				"schema.get_fr_sending_model_shows_test_20230425": []string{
					"prefix1_prefix2.schema.prefix2_ext_sending_sea_20230427",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin_freezed",
					"prefix1_prefix2.schema.prefix2_ext_ppin",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin",
				},
			},
		},
		"../test_data/get_fr_sending_model_shows_test_27072023.txt": {
			"sending_model_done_show_test_20230727": {
				"schema.get_fr_sending_model_shows_test_27072023": []string{
					"prefix1_prefix2.schema.prefix2_ext_sending_test_27072023",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin_freezed",
					"prefix1_prefix2.schema.prefix2_ext_ppin",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin",
				},
			},
		},
		"../test_data/get_fr_sending_model_shows.txt": {
			"prefix1_shows.schema.sending_model_done_show": {
				"schema.get_fr_sending_model_shows": []string{
					"prefix1_prefix2.schema.prefix2_ext_sending",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin",
					"prefix1_prefix2.schema.prefix2_ext_brzo_sending_ppin_freezed",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin_freezed",
					"prefix1_prefix2.schema.prefix2_ext_ppin",
				},
			},
		},
		"../test_data/get_fr_searching_pol_ppins_with_sending_show.txt": {
			"prefix1_shows.schema.fr_searching_pol_ppins_with_sending_show": {
				"schema.get_fr_searching_pol_ppins_with_sending_show": []string{
					"prefix1_prefix2.schema.prefix2_franchise",
					"prefix1_prefix2.schema.prefix2_ext_ppin",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin",
					"prefix1_prefix2.schema.prefix2_ext_sending",
				},
			},
		},
		"../test_data/get_fr_searching_pol_gc_show.txt": {
			"prefix1_shows.schema.fr_searching_pol_gc_show": {
				"schema.get_fr_searching_pol_gc_show": []string{
					"prefix1_shows.schema.shotout_iron_types",
					"prefix1_prefix2.schema.prefix2_drink_card_shells",
					"prefix1_prefix2.schema.prefix2_ext_ppin",
					"prefix1_prefix2.schema.prefix2_tables",
					"prefix1_prefix2.schema.prefix2_bazar",
				},
			},
		},
		"../test_data/get_fr_searching_pol_show_n.txt": {
			"prefix1_shows.schema.fr_searching_pol_show": {
				"schema.get_fr_searching_pol_show_n": []string{
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin_freezed",
					"prefix1_prefix2.schema.prefix2_ext_ppin",
					"prefix1_prefix2.schema.prefix2_tables",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_shows.schema.shotout_iron_types",
				},
			},
		},
		"../test_data/get_fr_searching_pol_show.txt": {
			"prefix1_shows.schema.fr_searching_pol_show": {
				"schema.get_fr_searching_pol_show": []string{
					"prefix1_prefix2.schema.prefix2_ext_ppin",
					"prefix1_prefix2.schema.prefix2_tables",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin_freezed",
					"prefix1_shows.schema.shotout_iron_types",
				},
			},
		},
		"../test_data/get_winds_show.txt": {
			"prefix1_shows.schema.winds_show": {
				"schema.get_winds_show": []string{
					"prefix1_prefix2.schema.prefix2_winds_history",
					"prefix1_prefix2.schema.prefix2_winds_files",
					"prefix1_prefix2.schema.prefix2_magic",
					"prefix1_prefix2.schema.excluded_winds",
				},
			},
		},
		"../test_data/get_extr_logs.txt": {
			"prefix1_shows.schema.extr_logs": {
				"schema.get_extr_logs": []string{
					"prefix1_ramm.schema.extr_logs_daily_errors_py_raw",
					"prefix1_ramm.schema.extr_logs_webetl_py_raw",
				},
			},
		},
		"../test_data/get_amg_frsome_shows.txt": {
			"prefix1_shows.schema.pp_unfiltered_irons": {
				"schema.get_amg_frsome_shows": []string{
					"prefix1_ramm.schema.inds_unfiltered_2",
				},
			},
			"prefix1_shows.schema.pp_unfiltered_irons_tableid_dim2": {
				"schema.get_amg_frsome_shows": []string{
					"prefix1_ramm.schema.inds_unfiltered_2",
				},
			},
			"prefix1_shows.schema.pp_unfiltered_icecream_tableid": {
				"schema.get_amg_frsome_shows": []string{
					"prefix1_ramm.schema.suc_unfiltered_2",
					"prefix1_ramm.schema.axe_jumping_shots",
					"prefix1_ramm.schema.axe_tickets",
					"prefix1_prefix2.schema.prefix2_tables",
					"prefix1_prefix2.schema.prefix2_date",
				},
			},
			"prefix1_shows.schema.pp_unfiltered_icecream": {
				"schema.get_amg_frsome_shows": []string{
					"prefix1_ramm.schema.suc_unfiltered_2",
					"prefix1_ramm.schema.rrr_pp_regs",
					"prefix1_prefix2.schema.prefix2_bazar",
				},
			},
		},
		"../test_data/get_trend_model_shows_bu_20201117.txt": {
			"prefix1_shows.schema.fr_searching_pol_show_bf": {
				"schema.get_trend_model_shows_bu_20201117": []string{
					"prefix1_prefix2.schema.prefix2_ext_ppin_bf",
					"prefix1_prefix2.schema.prefix2_tables",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_prefix2.schema.prefix2_date",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin",
				},
			},
			"prefix1_shows.schema.trend_model_searching_show": {
				"schema.get_trend_model_shows_bu_20201117": []string{
					"prefix1_shows.schema.fr_searching_pol_show_bf",
				},
			},
			"prefix1_shows.schema.trend_model_searching_show_bombies": {
				"schema.get_trend_model_shows_bu_20201117": []string{
					"prefix1_shows.schema.trend_model_searching_show",
					"prefix1_prefix3.schema.rrr_bombies",
				},
			},
			"prefix1_shows.schema.inds_unfiltered_trend_show": {
				"schema.get_trend_model_shows_bu_20201117": []string{
					"prefix1_ramm.schema.inds_unfiltered_2_trend_py_raw",
					"prefix1_ramm.schema.inds_unfiltered_2",
				},
			},
			"prefix1_shows.schema.suc_unfiltered_trend_show": {
				"schema.get_trend_model_shows_bu_20201117": []string{
					"prefix1_ramm.schema.suc_unfiltered_2_trend_py_raw",
					"prefix1_ramm.schema.suc_unfiltered_2",
				},
			},
			"prefix1_shows.schema.inds_unfiltered_trend_show_agg_tmp": {
				"schema.get_trend_model_shows_bu_20201117": []string{
					"prefix1_shows.schema.inds_unfiltered_trend_show",
					"prefix1_prefix2.schema.prefix2_date",
				},
			},
			"prefix1_shows.schema.suc_unfiltered_trend_show_agg_tmp": {
				"schema.get_trend_model_shows_bu_20201117": []string{
					"prefix1_shows.schema.suc_unfiltered_trend_show",
					"prefix1_prefix2.schema.prefix2_date",
				},
			},
			"prefix1_shows.schema.trend_model_pp_ticket_flossormace_show": {
				"schema.get_trend_model_shows_bu_20201117": []string{
					"prefix1_shows.schema.suc_unfiltered_trend_show",
					"prefix1_prefix2.schema.prefix2_date",
				},
			},
			"prefix1_shows.schema.trend_model_tickets_show": {
				"schema.get_trend_model_shows_bu_20201117": []string{
					"prefix1_ramm.schema.axe_tickets",
					"prefix1_ramm.schema.axe_ticket_pieceies",
					"prefix1_ramm.schema.axe_pieceies",
					"prefix1_shows.schema.trend_model_icecream_w_ticket_show",
				},
			},
			"prefix1_shows.schema.trend_model_elixirs_shotouts": {
				"schema.get_trend_model_shows_bu_20201117": []string{
					"prefix1_shows.schema.inds_unfiltered_trend_show_agg_tmp",
					"prefix1_shows.schema.suc_unfiltered_trend_show_agg_tmp",
				},
			},
			"prefix1_shows.schema.trend_model_all_key_rubrik": {
				"schema.get_trend_model_shows_bu_20201117": []string{
					"prefix1_shows.schema.trend_model_searching_show_bombies",
					"prefix1_shows.schema.trend_model_elixirs_shotouts",
					"prefix1_prefix2.schema.prefix2_date",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_prefix2.schema.prefix2_tables",
				},
			},
		},
		"../test_data/get_trend_model_shows_pwb_test.txt": {
			"prefix1_shows.schema.fr_searching_pol_show_bf_pwb_test": {
				"schema.get_trend_model_shows_pwb_test": []string{
					"prefix1_prefix2.schema.prefix2_ext_ppin_bf",
					"prefix1_prefix2.schema.prefix2_tables",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_prefix2.schema.prefix2_date",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin",
				},
			},
			"prefix1_shows.schema.trend_model_searching_show_bf_pwb_test": {
				"schema.get_trend_model_shows_pwb_test": []string{
					"prefix1_shows.schema.fr_searching_pol_show_bf_pwb_test",
					"prefix1_prefix2.schema.prefix2_ext_nukes_ppin",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_prefix2.schema.prefix2_tables",
				},
			},
			"prefix1_shows.schema.trend_model_searching_show_bombies_bf_pwb_test": {
				"schema.get_trend_model_shows_pwb_test": []string{
					"prefix1_shows.schema.trend_model_searching_show_bf_pwb_test",
					"prefix1_prefix3.schema.rrr_bombies",
				},
			},
			"prefix1_shows.schema.trend_model_elixirs_shotouts": {
				"schema.get_trend_model_shows_pwb_test": []string{
					"prefix1_shows.schema.sax_pp4_differenced_trend",
				},
			},
			"prefix1_shows.schema.trend_model_tickets_show": {
				"schema.get_trend_model_shows_pwb_test": []string{
					"prefix1_ramm.schema.axe_tickets",
					"prefix1_ramm.schema.axe_ticket_pieceies",
					"prefix1_ramm.schema.axe_pieceies",
				},
			},
			"prefix1_shows.schema.trend_model_shotouts_show": {
				"schema.get_trend_model_shows_pwb_test": []string{
					"prefix1_ramm.schema.axe_jumping_shots_bf",
					"prefix1_prefix2.schema.prefix2_date",
					"prefix1_ramm.schema.axe_tickets",
					"prefix1_prefix2.schema.prefix2_tables",
				},
			},
			"prefix1_shows.schema.trend_model_all_key_rubrik_bf_pwb_test": {
				"schema.get_trend_model_shows_pwb_test": []string{
					"prefix1_shows.schema.trend_model_searching_show_bombies_bf_pwb_test",
					"prefix1_shows.schema.trend_model_elixirs_shotouts",
					"prefix1_shows.schema.trend_model_shotouts_show",
					"prefix1_prefix2.schema.prefix2_date",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_prefix2.schema.prefix2_tables",
				},
			},
		},
		"../test_data/get_trend_model_shows.txt": {
			"prefix1_shows.schema.fr_searching_pol_show_bf": {
				"schema.get_trend_model_shows": []string{
					"prefix1_prefix2.schema.prefix2_ext_ppin_bf",
					"prefix1_prefix2.schema.prefix2_tables",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_prefix2.schema.prefix2_date",
					"prefix1_prefix2.schema.prefix2_ext_brzo_ppin",
				},
			},
			"prefix1_shows.schema.trend_model_searching_show": {
				"schema.get_trend_model_shows": []string{
					"prefix1_shows.schema.fr_searching_pol_show_bf",
					"prefix1_prefix2.schema.prefix2_ext_nukes_ppin",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_prefix2.schema.prefix2_tables",
				},
			},
			"prefix1_shows.schema.trend_model_searching_show_bombies": {
				"schema.get_trend_model_shows": []string{
					"prefix1_shows.schema.trend_model_searching_show",
					"prefix1_prefix3.schema.rrr_bombies",
				},
			},
			"prefix1_shows.schema.trend_model_elixirs_shotouts": {
				"schema.get_trend_model_shows": []string{
					"prefix1_shows.schema.sax_pp4_differenced_trend",
				},
			},
			"prefix1_shows.schema.trend_model_tickets_show": {
				"schema.get_trend_model_shows": []string{
					"prefix1_ramm.schema.axe_tickets",
					"prefix1_ramm.schema.axe_ticket_pieceies",
					"prefix1_ramm.schema.axe_pieceies",
				},
			},
			"prefix1_shows.schema.trend_model_shotouts_show": {
				"schema.get_trend_model_shows": []string{
					"prefix1_ramm.schema.axe_jumping_shots_bf",
					"prefix1_prefix2.schema.prefix2_date",
					"prefix1_ramm.schema.axe_tickets",
					"prefix1_prefix2.schema.prefix2_tables",
				},
			},
			"prefix1_shows.schema.trend_model_all_key_rubrik": {
				"schema.get_trend_model_shows": []string{
					"prefix1_shows.schema.trend_model_searching_show_bombies",
					"prefix1_shows.schema.trend_model_elixirs_shotouts",
					"prefix1_shows.schema.trend_model_shotouts_show",
					"prefix1_prefix2.schema.prefix2_date",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_prefix2.schema.prefix2_tables",
				},
			},
		},
		"../test_data/get_www_ld_logs.txt": {
			"prefix1_shows.schema.www_ld": {
				"schema.get_www_ld_logs": []string{
					"prefix1_prefix2.schema.v_www_ld",
				},
			},
		},
		"../test_data/get_wwws_ld_data.txt": {
			"prefix1_shows.schema.www_ld": {
				"schema.get_wwws_ld_data": []string{
					"prefix1_prefix2.schema.v_www_ld",
				},
			},
		},
		"../test_data/get_farm_shotouts_show.txt": {
			"farm_shotouts_show": {
				"schema.get_farm_shotouts_show": []string{
					"prefix1_prefix2.schema.prefix2_shotouts",
				},
			},
		},
		"../test_data/get_farm_day_differenced_shows_bu_20201114.txt": {
			"prefix1_shows.schema.farm_bazars": {
				"schema.get_farm_day_differenced_shows_bu_20201114": []string{
					"prefix1_prefix2.schema.prefix2_bazar",
				},
			},
			"prefix1_shows.schema.farm_countries": {
				"schema.get_farm_day_differenced_shows_bu_20201114": []string{
					"prefix1_prefix2.schema.prefix2_magic",
				},
			},
			"prefix1_shows.schema.farm_tables_with_state": {
				"schema.get_farm_day_differenced_shows_bu_20201114": []string{
					"prefix1_ramm.schema.axe_tables",
					"prefix1_prefix2.schema.prefix2_tables",
					"prefix1_ramm.schema.axe_states",
					"prefix1_prefix2.schema.prefix2_franchise",
					"prefix1_ramm.schema.axe_users",
				},
			},
			"prefix1_shows.schema.farm_table_pieceies": {
				"schema.get_farm_day_differenced_shows_bu_20201114": []string{
					"prefix1_prefix2.schema.prefix2_table_pieceies",
				},
			},
			"prefix1_shows.schema.farm_pieceies_with_state": {
				"schema.get_farm_day_differenced_shows_bu_20201114": []string{
					"prefix1_prefix2.schema.prefix2_pieceies",
					"prefix1_prefix2.schema.prefix2_state_pieceies",
				},
			},
			"prefix1_shows.schema.farm_day_differenced_elixirs_shotouts_dim": {
				"schema.get_farm_day_differenced_shows_bu_20201114": []string{
					"prefix1_shows.schema.pp_unfiltered_irons_tableid_dim2",
					"prefix1_shows.schema.pp_unfiltered_icecream_tableid",
				},
			},
			"prefix1_shows.schema.farm_fr_searching_merged_show": {
				"schema.get_farm_day_differenced_shows_bu_20201114": []string{
					"prefix1_prefix2.schema.prefix2_fr_manual_ppin",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_prefix3.schema.rrr_bombies",
					"prefix1_shows.schema.fr_searching_pol_show",
					"prefix1_prefix2.schema.prefix2_date",
					"prefix1_prefix2.schema.prefix2_webbazar_lookup",
				},
			},
			"prefix1_shows.schema.farm_fr_searching_merged_show_bombies": {
				"schema.get_farm_day_differenced_shows_bu_20201114": []string{
					"prefix1_shows.schema.farm_fr_searching_merged_show",
					"prefix1_prefix3.schema.rrr_bombies",
					"prefix1_shows.schema.farm_day_differenced_elixirs_shotouts_dim",
					"prefix1_shows.schema.farm_tables_with_state",
				},
			},
			"prefix1_shows.schema.farm_all_key_rubrik": {
				"schema.get_farm_day_differenced_shows_bu_20201114": []string{
					"prefix1_shows.schema.farm_fr_searching_merged_show_bombies",
					"prefix1_shows.schema.farm_day_differenced_elixirs_shotouts_dim",
					"prefix1_prefix2.schema.prefix2_date",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_prefix2.schema.prefix2_tables",
				},
			},
		},
		"../test_data/get_farm_day_differenced_shows.txt": {
			"prefix1_shows.schema.farm_tickets": {
				"schema.get_farm_day_differenced_shows": []string{
					"prefix1_prefix2.schema.prefix2_tickets",
				},
			},
			"prefix1_shows.schema.farm_bazars": {
				"schema.get_farm_day_differenced_shows": []string{
					"prefix1_prefix2.schema.prefix2_bazar",
				},
			},
			"prefix1_shows.schema.farm_countries": {
				"schema.get_farm_day_differenced_shows": []string{
					"prefix1_prefix2.schema.prefix2_magic",
				},
			},
			"prefix1_shows.schema.farm_tables_with_state": {
				"schema.get_farm_day_differenced_shows": []string{
					"prefix1_prefix2.schema.prefix2_tables",
					"prefix1_ramm.schema.axe_states",
					"prefix1_prefix2.schema.prefix2_franchise",
					"prefix1_ramm.schema.axe_users",
				},
			},
			"prefix1_shows.schema.farm_table_pieceies": {
				"schema.get_farm_day_differenced_shows": []string{
					"prefix1_prefix2.schema.prefix2_table_pieceies",
				},
			},
			"prefix1_shows.schema.farm_pieceies_with_state": {
				"schema.get_farm_day_differenced_shows": []string{
					"prefix1_prefix2.schema.prefix2_pieceies",
					"prefix1_prefix2.schema.prefix2_state_pieceies",
				},
			},
			"prefix1_shows.schema.farm_day_differenced_elixirs_shotouts_dim": {
				"schema.get_farm_day_differenced_shows": []string{
					"prefix1_shows.schema.sax_pp4_differenced",
				},
			},
			"prefix1_shows.schema.farm_fr_searching_merged_show": {
				"schema.get_farm_day_differenced_shows": []string{
					"prefix1_prefix2.schema.prefix2_ext_nukes_ppin",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_prefix2.schema.prefix2_tables",
					"prefix1_prefix2.schema.prefix2_fr_manual_ppin",
					"prefix1_prefix3.schema.rrr_bombies",
					"prefix1_shows.schema.fr_searching_pol_show",
					"prefix1_prefix2.schema.prefix2_webbazar_lookup",
				},
			},
			"prefix1_shows.schema.farm_fr_searching_merged_show_bombies": {
				"schema.get_farm_day_differenced_shows": []string{
					"prefix1_shows.schema.farm_fr_searching_merged_show",
					"prefix1_prefix3.schema.rrr_bombies",
					"prefix1_shows.schema.farm_day_differenced_elixirs_shotouts_dim",
					"prefix1_shows.schema.farm_tables_with_state",
				},
			},
			"prefix1_shows.schema.farm_all_key_rubrik": {
				"schema.get_farm_day_differenced_shows": []string{
					"prefix1_shows.schema.farm_fr_searching_merged_show_bombies",
					"prefix1_shows.schema.farm_day_differenced_elixirs_shotouts_dim",
					"prefix1_shows.schema.farm_shotouts_show",
					"prefix1_prefix2.schema.prefix2_date",
					"prefix1_prefix2.schema.prefix2_bazar",
					"prefix1_prefix2.schema.prefix2_tables",
				},
			},
		},
		"../test_data/get_prefix1_axe.txt": {
			"prefix1_ramm.schema.axe_franchise": {
				"schema.get_prefix1_axe": []string{
					"msp.franchise",
				},
			},
			"prefix1_ramm.schema.axe_options": {
				"schema.get_prefix1_axe": []string{
					"msp.options",
				},
			},
			"prefix1_ramm.schema.axe_ice_jumping_data": {
				"schema.get_prefix1_axe": []string{
					"msp.ice_jumping_data",
				},
			},
			"prefix1_ramm.schema.axe_ices": {
				"schema.get_prefix1_axe": []string{
					"msp.ices",
				},
			},
			"prefix1_ramm.schema.axe_pieceies": {
				"schema.get_prefix1_axe": []string{
					"msp.pieceies",
				},
			},
			"prefix1_ramm.schema.axe_ticket_pieceies": {
				"schema.get_prefix1_axe": []string{
					"msp.ticket_pieceies",
				},
			},
			"prefix1_ramm.schema.axe_industries": {
				"schema.get_prefix1_axe": []string{
					"msp.industries",
				},
			},
			"prefix1_ramm.schema.axe_states": {
				"schema.get_prefix1_axe": []string{
					"msp.states",
				},
			},
			"prefix1_ramm.schema.axe_pdf": {
				"schema.get_prefix1_axe": []string{
					"msp.pdf",
				},
			},
			"prefix1_ramm.schema.axe_table_pieceies": {
				"schema.get_prefix1_axe": []string{
					"msp.table_pieceies",
				},
			},
			"prefix1_ramm.schema.axe_tables": {
				"schema.get_prefix1_axe": []string{
					"msp.tables",
				},
			},
			"prefix1_ramm.schema.axe_bazars": {
				"schema.get_prefix1_axe": []string{
					"msp.bazars",
				},
			},
			"prefix1_ramm.schema.axe_static_irons": {
				"schema.get_prefix1_axe": []string{
					"msp.static_irons",
				},
			},
			"prefix1_ramm.schema.axe_users": {
				"schema.get_prefix1_axe": []string{
					"msp.users",
				},
			},
			"prefix1_ramm.schema.axe_drink_cards": {
				"schema.get_prefix1_axe": []string{
					"msp.drink_cards",
				},
			},
			"prefix1_ramm.schema.axe_drink_card_shells": {
				"schema.get_prefix1_axe": []string{
					"msp.drink_card_shells",
				},
			},
			"prefix1_ramm.schema.axe_sortings_raw": {
				"schema.get_prefix1_axe": []string{
					"msp.sortings",
				},
			},
			"prefix1_ramm.schema.axe_jumping_users": {
				"schema.get_prefix1_axe": []string{
					"msp.jumping_users",
				},
			},
			"prefix1_ramm.schema.axe_fug": {
				"schema.get_prefix1_axe": []string{
					"msp.fug",
				},
			},
		},
		"../test_data/get_tag_hits_model_shows.txt": {
			"prefix1_shows.schema.tag_model_day_hits": {
				"schema.get_tag_hits_model_shows": []string{
					"prefix1_prefix2.schema.prefix2_tag_hits",
					"prefix1_prefix2.schema.prefix2_date",
				},
			},
			"prefix1_shows.schema.tag_model_tags": {
				"schema.get_tag_hits_model_shows": []string{
					"prefix1_prefix2.schema.prefix2_tags",
					"prefix1_prefix2.schema.prefix2_bazar",
				},
			},
			"prefix1_shows.schema.tag_model_table_pieceies": {
				"schema.get_tag_hits_model_shows": []string{
					"prefix1_prefix2.schema.prefix2_table_pieceies",
				},
			},
			"prefix1_shows.schema.tag_model_pieceies": {
				"schema.get_tag_hits_model_shows": []string{
					"prefix1_prefix2.schema.prefix2_pieceies",
				},
			},
			"prefix1_shows.schema.farm_date": {
				"schema.get_tag_hits_model_shows": []string{
					"prefix1_prefix2.schema.prefix2_date",
				},
			},
		},
		"../test_data/insert_dividend_ttt_exchanges.txt": {
			"prefix1_prefix2.schema.prefix2_ttt_dividendted_exchanges": {
				"schema.insert_dividend_ttt_exchanges": []string{
					"prefix2_ttt_dividendted_exchanges",
					"prefix1_prefix2.schema.prefix2_fr_ttt",
					"prefix1_prefix2.schema.prefix2_tables",
					"prefix1_prefix3.schema.rrr_bombies",
				},
			},
		},
		"../test_data/table_variable_extraction.txt": {
			"prefix1_shows.schema.table_test": {
				"schema.table_variable_extraction": []string{
					"other_source",
					"prefix1_prefix2.schema.test2",
					"prefix1_shows.schema.table_test3",
					"db.schema.something",
				},
			},
			"prefix1_shows.schema.table_test6": {
				"schema.table_variable_extraction": []string{
					"prefix1_shows.schema.table_test3",
				},
			},
		},
		"../test_data/t_test.txt": {
			"some_schema.dbo.ddd_oranges_raw": {
				"dbo.transform_ddd_oranges_1_add_history": []string{
					"some_schema.dbo.ddd_oranges_files",
					"some_schema.dbo.ddd_magic",
					"some_schema.dbo.ddd_oranges",
				},
			},
			"some_schema.dbo.ddd_oranges_history": {
				"dbo.transform_ddd_oranges_1_add_history": []string{
					"some_schema.dbo.ddd_oranges_raw",
				},
			},
		},
		"../test_data/merge_sp.txt": {
			"products": {
				"schema.merge_sp": []string{
					"updatedproducts",
				},
			},
			"products_2": {
				"schema.merge_sp": []string{
					"updatedproducts",
				},
			},
			"products_3": {
				"schema.merge_sp": []string{
					"updatedproducts",
				},
			},
			"products_4": {
				"schema.merge_sp": []string{
					"updatedproducts",
				},
			},
		},
		"../test_data/merge_sp_sub.txt": {
			"products": {
				"schema.merge_sp": []string{
					"dbo.test",
				},
			},
			"products_2": {
				"schema.merge_sp": []string{
					"dbo.test",
				},
			},
		},
		"../test_data/merge_sp_test.txt": {
			"c": {
				"schema.merge_sp_test": []string{
					"a",
				},
			},
		},
	}
	for testFile, expectedResult := range tests {
		file, err := os.Open(testFile)
		if err != nil {
			panic(err)
		}
		defer file.Close()
		spSlice := []io.Reader{file}
		result, err := GetLineage(spSlice)
		if err != nil {
			panic(err)
		}
		fmt.Println(result)
		fmt.Println("")
		// Implement custom DeepEqual that ignores shell
		for sinkTable, lineage := range expectedResult {
			resultLineage, ok := result[sinkTable]
			if !ok {
				t.Errorf("The sink table name is missing: %v", sinkTable)
			}
			for spName, sources := range lineage {
				resultSources, ok := resultLineage[spName]
				if !ok {
					t.Errorf(
						"The stored procedure name %v from sink table %v is missing",
						spName,
						sinkTable,
					)
				}
				if len(resultSources) != len(sources) {
					t.Errorf(
						"The number of sources of stored procedure %v and table %v does not match. Expected: %v, Got: %v",
						spName,
						sinkTable,
						len(sources),
						len(resultSources),
					)
				}
				for _, source := range sources {
					if !slices.Contains(resultSources, source) {
						t.Errorf("The expected source is missing: %v", source)
					}
				}
			}
		}

		for sinkTable := range result {
			_, ok := expectedResult[sinkTable]
			if !ok {
				t.Errorf("This sink table name is not expected: %v", sinkTable)
			}
		}
	}
}
