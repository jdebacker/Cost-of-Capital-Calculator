'''
------------------------------------------------------------------------
Last updated 4/9/2016

This program reads in the SOI business entity data and does some
pre-processing of these data to get them ready to combine with other
data sources:
1) Creates industry groupings consistent with BEA fixed asset data
2) Splits C and S corp data
3) Apportions partnership data across type of partners
4) Adds data on farm sole proprietors to non-farm sole prop data


This py-file calls the following other file(s):
            


This py-file creates the following other file(s):

------------------------------------------------------------------------
'''
# Packages:
import os.path
import sys
import numpy as np
import pandas as pd
import xlrd
# # Directories:
# _CUR_DIR = os.path.dirname(__file__)
# _OUT_DIR = os.path.join(_CUR_DIR, 'output')
# _SOI_DIR = os.path.join(_OUT_DIR, 'soi')
# # Importing custom modules
# import naics_processing as naics
# import constants as cst
# # Importing soi tax data helper custom modules
# import pull_soi_corp as corp
# import pull_soi_partner as prt
# import pull_soi_proprietorship as prop
# # Dataframe names:
# _TOT_CORP_DF_NM = cst.TOT_CORP_DF_NM
# _S_CORP_DF_NM = cst.S_CORP_DF_NM
# _C_CORP_DF_NM = cst.C_CORP_DF_NM
# _INC_DF_NM = cst.INC_PRT_DF_NM
# _AST_DF_NM = cst.AST_PRT_DF_NM
# _TYP_DF_NM = cst.TYP_PRT_DF_NM
# _NFARM_DF_NM = cst.NON_FARM_PROP_DF_NM
# _FARM_DF_NM = cst.FARM_PROP_DF_NM
# #
# _ALL_SECTORS = cst.ALL_SECTORS_NMS_LIST
# _ALL_SECTORS_DICT = cst.ALL_SECTORS_NMS_DICT


# _TOT_CORP_IN_FILE = fp.get_file(dirct=_CORP_DIR, contains=[_YR+"sb1.csv"])
# _S_CORP_IN_FILE = fp.get_file(dirct=_CORP_DIR, contains=[_YR+"sb3.csv"])

all_corp_path = '/Users/jasondebacker/repos/B-Tax/Python/btax/depreciation/data/raw_data/soi/soi_corporate/2011sb1.csv'
s_corp_path = '/Users/jasondebacker/repos/B-Tax/Python/btax/depreciation/data/raw_data/soi/soi_corporate/2011sb3.csv'
s_corp_path = '/Users/jasondebacker/repos/B-Tax/Python/btax/depreciation/data/raw_data/soi/soi_corporate/2011sb3.csv'
part_netinc_path = '/Users/jasondebacker/repos/B-Tax/Python/btax/depreciation/data/raw_data/soi/soi_partner/12pa01.xls'
part_deprec_path = '/Users/jasondebacker/repos/B-Tax/Python/btax/depreciation/data/raw_data/soi/soi_partner/12pa03.xlsx'
part_split_path = '/Users/jasondebacker/repos/B-Tax/Python/btax/depreciation/data/raw_data/soi/soi_partner/12pa05.xls'
soleprop_path = '/Users/jasondebacker/repos/B-Tax/Python/btax/depreciation/data/raw_data/soi/soi_proprietorship/12sp01br.xls'
farm_path = '/Users/jasondebacker/repos/B-Tax/Python/btax/depreciation/data/raw_data/soi/soi_proprietorship/farm_data.xlsx'
soi_codes_path = '/Users/jasondebacker/repos/B-Tax/Python/btax/depreciation/data/raw_data/soi/soi_industry_codes.csv'

datapaths = {'all_corp': all_corp_path, 's_corp': s_corp_path, 'part_netinc': part_netinc_path, 'part_deprec':part_deprec_path,
             'part_split':part_split_path, 'soleprop':soleprop_path, 'farm':farm_path, 'soi_codes':soi_codes_path}
def get_soi_data(datapaths):
    '''
    Gathers together SOI data by tax entity type

    Inputs:
        datapaths = dictionary of strings containing paths to raw SOI data
        
    Output:
        soi_data = dictionary of dataframes, one for each tax entity type
    '''

    # separate c and s corp
    # load raw data
    try:
        all_corp = pd.read_csv(datapaths['all_corp']).fillna(0)
        # keep only totals (not values for each asset class)
        all_corp = all_corp.drop(all_corp[all_corp['AC']> 1.].index)
        # drop total across all industries
        all_corp = all_corp.drop(all_corp[all_corp['INDY_CD']== 1.].index)

        corp_data_numeric_vars_names = ['TOT_ASSTS_IND','TOT_ASSTS','CASH_IND','CASH','TRD_NOTES_ACCTS_RCV_IND',
                      'TRD_NOTES_ACCTS_RCV','BAD_DBT_ALLW_IND','BAD_DBT_ALLW','INVNTRY_IND','INVNTRY','GVT_OBLGNS_IND',
                      'GVT_OBLGNS','TX_EXMT_SEC_IND','TX_EXMT_SEC','OTHR_CUR_ASSTS_IND','OTHR_CUR_ASSTS','LNS_TO_STKHDR_IND',
                      'LNS_TO_STKHDR','MRTG_RE_LNS_IND','MRTG_RE_LNS','OTHR_INVSMTS_IND','OTHR_INVSMTS','DPRCBL_ASSTS_IND',
                      'DPRCBL_ASSTS','ACCUM_DPR_IND','ACCUM_DPR','DPTBL_ASSTS_IND','DPTBL_ASSTS','ACCUM_DPLTN_IND','ACCUM_DPLTN',
                      'LAND_IND','LAND','INTNGBL_ASSTS_IND','INTNGBL_ASSTS','ACCUM_AMORT_IND','ACCUM_AMORT','COMP_OTHR_ASSTS_IND',
                      'COMP_OTHR_ASSTS','TOT_LBLTS_IND','TOT_LBLTS','ACCTS_PYBL_IND','ACCTS_PYBL','MRTG_LT_1YR_IND','MRTG_LT_1YR',
                      'COMP_OTHR_CUR_LBLTS_IND','COMP_OTHR_CUR_LBLTS','LNS_FRM_STKHDR_IND','LNS_FRM_STKHDR','MRTG_GT_1YR_IND',
                      'MRTG_GT_1YR','COMP_OTHR_LBLTS_IND','COMP_OTHR_LBLTS','COMP_NET_WRTH_IND','COMP_NET_WRTH','CAP_STCK_IND',
                      'CAP_STCK','PD_CAP_SRPLS_IND','PD_CAP_SRPLS','RTND_ERNGS_APPR_IND','RTND_ERNGS_APPR','COMP_RTND_ERNGS_UNAPPR_IND',
                      'COMP_RTND_ERNGS_UNAPPR','CST_TRSRY_STCK_IND','CST_TRSRY_STCK','COMP_TOT_RCPTS_IND','COMP_TOT_RCPTS',
                      'GRS_RCPTS_IND','GRS_RCPTS','INTRST_IND','INTRST','COMP_TX_EXMT_INTRST_IND','COMP_TX_EXMT_INTRST',
                      'COMP_GRS_RNTS_IND','COMP_GRS_RNTS','GRS_RYLTS_IND','GRS_RYLTS','D_NET_STCG_IND','D_NET_STCG',
                      'D_NET_LTCG_TOT_IND','D_NET_LTCG_TOT','NET_GN_LSS_POS_IND','NET_GN_LSS_POS','COMP_DIV_DOM_CORP_IND',
                      'COMP_DIV_DOM_CORP','COMP_DIV_FRN_CORP_IND','COMP_DIV_FRN_CORP','COMP_OTHR_RCPTS_IND','COMP_OTHR_RCPTS',
                      'COMP_TOT_DED_IND','COMP_TOT_DED','CST_OF_GDS_IND','CST_OF_GDS','CMPNSTN_OFFCRS_IND','CMPNSTN_OFFCRS',
                      'SLRS_WGS_IND','SLRS_WGS','RPRS_IND','RPRS','BAD_DBT_DED_IND','BAD_DBT_DED','RNTS_PD_IND','RNTS_PD',
                      'TX_PD_IND','TX_PD','INTRST_PD_IND','INTRST_PD','CNTRBTNS_IND','CNTRBTNS','TOT_AMORT_IND','TOT_AMORT',
                      'NET_DPR_IND','NET_DPR','DPLTN_IND','DPLTN','ADVRTSNG_IND','ADVRTSNG','PNSN_PRFT_SHRNG_PLNS_IND',
                      'PNSN_PRFT_SHRNG_PLNS','EMP_BNFT_PRG_IND','EMP_BNFT_PRG','DP_PROD_ACTVTY_DED_IND','DP_PROD_ACTVTY_DED',
                      'NET_GN_LSS_NEG_IND','NET_GN_LSS_NEG','COMP_OTHR_DED_IND','COMP_OTHR_DED','COMP_TOT_RCPTS_LS_TOT_DED_IND',
                      'COMP_TOT_RCPTS_LS_TOT_DED','COMP_TXBL_INCM_RFC_IND','COMP_TXBL_INCM_RFC','NET_INCM_IND','NET_INCM',
                      'NET_INCM_POS_IND','NET_INCM_POS','NET_INCM_NEG_IND','NET_INCM_NEG','NET_INCM_S_IND','NET_INCM_S',
                      'COMP_TOT_STAT_SPCL_DED_IND','COMP_TOT_STAT_SPCL_DED','NOLD_IND','NOLD','COMP_DIV_RCVD_DED_IND',
                      'COMP_DIV_RCVD_DED','COMP_INCM_SBJ_TX_TOT_IND','COMP_INCM_SBJ_TX_TOT','COMP_TX_BFR_CRS_IND',
                      'COMP_TX_BFR_CRS','INCM_TX_IND','INCM_TX','ALT_MIN_TX_IND','ALT_MIN_TX','FRN_TX_CR_IND','FRN_TX_CR',
                      'GEN_BUS_CR_IND','GEN_BUS_CR','MIN_TX_CR_IND','MIN_TX_CR','COMP_TX_AFTR_CRS_IND','COMP_TX_AFTR_CRS',
                      '(cash_dist + prpty_dist)_IND','(cash_dist + prpty_dist)','STCK_DIST_IND','STCK_DIST']
        corp_data_variables_of_interest = ['TOT_ASSTS','DPRCBL_ASSTS','ACCUM_DPR']
    except IOError:
        print "IOError: SOI total corp data file not found."
        return None
    try:
        s_corp = pd.read_csv(datapaths['s_corp']).fillna(0)
        # keep only totals (not values for each asset class)
        s_corp = s_corp.drop(s_corp[s_corp['AC']> 1.].index)
        # drop total across all industries
        s_corp = s_corp.drop(s_corp[s_corp['INDY_CD']== 1.].index)


    except IOError:
        print "IOError: SOI S-corp data file not found."
        return None

    #load industry codes
    soi_ind_codes = pd.read_csv(datapaths['soi_codes']).fillna(0)

    # merge codes to corp data
    all_corp = pd.merge(all_corp, soi_ind_codes, how='inner', left_on=['INDY_CD'], right_on=['minor_code_alt'],
      left_index=False, right_index=False, sort=False,
      suffixes=('_x', '_y'), copy=True, indicator=True)
    # keep only rows that match in both datasets - this should keep only unique soi minor industries
    all_corp = all_corp.drop(all_corp[all_corp['_merge']!='both' ].index)

    corp_ratios = all_corp[['INDY_CD','minor_code_alt','minor_code','major_code','sector_code']]
    for var in corp_data_variables_of_interest :
        corp_ratios[var+'_ratio'] = all_corp.groupby(['sector_code'])[var].apply(lambda x: x/float(x.sum()))

    #print corp_ratios['sector_code'][:5], corp_ratios['minor_code_alt'][:5],    corp_ratios['TOT_ASSTS_ratio'][:5]

    # new data w just ratios that will then merge to s corp data by sector code (many to one merge)
    # first just keep s corp columns want
    s_corp = s_corp[['INDY_CD']+corp_data_variables_of_interest]
    # merge ratios to s corp data
    s_corp = pd.merge(corp_ratios, s_corp, how='left', left_on=['sector_code'], right_on=['INDY_CD'],
      left_index=False, right_index=False, sort=False,
      suffixes=('_x', '_y'), copy=True, indicator=True)

    #calculate s corp values by minor industry using ratios
    for var in corp_data_variables_of_interest :
        s_corp[var] = s_corp[var]*s_corp[var+'_ratio']
    print s_corp.head(n=5)

    # now create c corp data by subtracting s_corp from all_corp
    # first merge s_corp and all_corp
    s_corp = s_corp.drop(['_merge'], axis=1)
    all_corp = all_corp.drop(['_merge'], axis=1)
    all_merged = pd.merge(all_corp, s_corp, how='inner', left_on=['minor_code_alt'], right_on=['minor_code_alt'],
      left_index=False, right_index=False, sort=False,
      suffixes=('_x', '_y'), copy=True, indicator=True)
    # c_corp = all_merged[['INDY_CD','minor_code_alt','minor_code','major_code','sector_code']]
    c_corp = all_merged[['INDY_CD','minor_code_alt']]
    for var in corp_data_variables_of_interest :
        c_corp[var] = all_merged[var+'_x']-all_merged[var+'_y']
    #print c_corp.head(n=5)


    soi_data = {'c_corp':c_corp, 's_corp':s_corp}

    return soi_data

df_dict = get_soi_data(datapaths)
print df_dict['s_corp'].describe()
print df_dict['c_corp'].describe()


def load_corporate(soi_tree,
                   from_out=False, get_all=False,
                   get_tot=False, get_s=False, get_c=False,
                   output_data=False, out_path=None):
    """ Loading the corporate tax soi data into a NAICS Tree.
    
    :param soi_tree: The NAICS tree to put all of the data in.
    :param from_out: If the corporate soi data is already in an output folder,
           then it can be read in directly from the output.
    :param get_all: Get corporate soi data for all kinds of corporations.
    :param get_tot: Get the aggregate soi data for corporations.
    :param get_s: Get the soi data for s corporations.
    :param get_c: Interpolate the soi data for c corporations.
    :param output_data: Print the corporate dataframes to csv files in the
           output folder.
    :param out_path: The output_path, both for reading in output data and for
           printing to the output file
    
    .. note: Because there is only data on the aggregate and s corporations,
       the c corporations data can only be interpolated if the other two have
       been calculated.
    """
    # Initializing the output path:
    if out_path == None:
        out_path = _SOI_DIR
    # Initializing booleans based of initial input booleans:
    if get_all:
        get_tot = True
        get_s = True
        get_c = True
    if not get_tot or not get_s:
        get_c = False
    # Load the total corporate soi data into the NAICS tree:
    if get_tot:
        soi_tree = corp.load_soi_tot_corp(data_tree=soi_tree,
                                          from_out=from_out)
        if output_data:
            naics.print_tree_dfs(tree=soi_tree, out_path=out_path,
                                 data_types=[_TOT_CORP_DF_NM])
    # Load the S-corporate soi data into the NAICS tree:
    if get_s:
        soi_tree = corp.load_soi_s_corp(data_tree=soi_tree,
                                        from_out=from_out)
        if output_data:
            naics.print_tree_dfs(tree=soi_tree, out_path=out_path,
                                 data_types=[_S_CORP_DF_NM])
    # Calculate the C-corporate soi data for the NAICS tree:
    if get_c:
        soi_tree = corp.calc_c_corp(data_tree=soi_tree,
                                    from_out=from_out)
        if output_data:
            naics.print_tree_dfs(tree=soi_tree, out_path=out_path,
                                 data_types=[_C_CORP_DF_NM])
    return soi_tree
    

def load_partner(soi_tree,
                 from_out=False, output_data=False,
                 out_path=None):
    """ Loading the partnership tax soi data into a NAICS Tree.
    
    :param soi_tree: The NAICS tree to put all of the data in.
    :param from_out: If the corporate soi data is already in an output file,
           then it can be read in directly from the output.
    :param output_data: Print the corporate dataframes to csv files in the
           output folder.
    :param out_path: The output_path, both for reading in output data and for
           printing to the output file
    """
    # Initializing the output path:
    if out_path == None:
        out_path = _SOI_DIR
    # Load the soi income data into the NAICS tree:
    soi_tree = prt.load_income(data_tree=soi_tree, from_out=from_out)
    # Load the soi asset data into the NAICS tree:
    soi_tree = prt.load_asset(data_tree=soi_tree, from_out=from_out)
    # Load the soi partnership types data into the NAICS tree:
    soi_tree = prt.load_type(data_tree=soi_tree, from_out=from_out)
    # Output the data to csv files in the output folder:
    if output_data:
        naics.print_tree_dfs(tree=soi_tree, out_path=out_path,
                             data_types=[_INC_DF_NM, _AST_DF_NM, _TYP_DF_NM])
    return soi_tree


def load_proprietorship(soi_tree,
                       from_out=False, get_all=False,
                       get_nonfarm=False, get_farm=False,
                       output_data=False, out_path=None):
    """ Loading the proprietorship tax soi data into a NAICS Tree.
    
    :param soi_tree: The NAICS tree to put all of the data in.
    :param from_out: If the corporate soi data is already in an output file,
           then it can be read in directly from the output.
    :param output_data: Print the corporate dataframes to csv files in the
           output folder.
    :param out_path: The output_path, both for reading in output data and for
           printing to the output file
    """
    # Initializing the output path:
    if out_path == None:
        out_path = _SOI_DIR
    # Load the soi nonfarm data into the NAICS tree:
    if get_nonfarm:
        soi_tree = prop.load_soi_nonfarm_prop(
                                    data_tree=soi_tree, from_out=from_out
                                    )
    # Load the farm data into to the NAICS tree:
    if get_farm:
        soi_tree = prop.load_soi_farm_prop(
                                    data_tree=soi_tree, from_out=from_out
                                    )
    # Output the data to csv files in the output folder:
    if output_data:
            naics.print_tree_dfs(tree=soi_tree, out_path=out_path,
                                 data_types=[_NFARM_DF_NM, _FARM_DF_NM])
    return soi_tree


def calc_assets(soi_tree, asset_tree):
    """ Calculating a breakdown of the various sector type's assets
    into fixed assets, inventories, and land. 
    
    :param asset_tree: The NAICS tree to put all of the data in.
    :param soi_tree: A NAICS tree containing all the pertinent soi data.
    """
    # Initializing dataframes for all NAICS industries:
    asset_tree.append_all(df_nm="FA", df_cols=_ALL_SECTORS)
    asset_tree.append_all(df_nm="INV", df_cols=_ALL_SECTORS)
    asset_tree.append_all(df_nm="LAND", df_cols=_ALL_SECTORS)
    # Calculate fixed assets, inventories, and land for each industry/sector
    for i in range(0, len(asset_tree.enum_inds)):
        cur_dfs = soi_tree.enum_inds[i].data.dfs
        out_dfs = asset_tree.enum_inds[i].data.dfs
        # Total of all the partner data for the current industry:
        partner_sum = sum(cur_dfs[_TYP_DF_NM].iloc[0,:]) 
        # C-Corporations:
        sector = _ALL_SECTORS_DICT["C_CORP"]
        cur_df = cur_dfs[_C_CORP_DF_NM]
        out_dfs["FA"][sector][0] = cur_df["depreciable_assets"][0]
        out_dfs["INV"][sector][0] = cur_df["inventories"][0]
        out_dfs["LAND"][sector][0] = cur_df["land"][0]
        # S-Corporations:
        sector = _ALL_SECTORS_DICT["S_CORP"]
        cur_df = cur_dfs[_S_CORP_DF_NM]
        out_dfs["FA"][sector][0] = cur_df["depreciable_assets"][0]
        out_dfs["INV"][sector][0] = cur_df["inventories"][0]
        out_dfs["LAND"][sector][0] = cur_df["land"][0]
        # Partnership sectors:
        for sector in cst.DFLT_PRT_TYP_DF_COL_NMS_DICT.values():
            if partner_sum != 0:
                ratio = abs(float(cur_dfs[_TYP_DF_NM][sector][0]))/partner_sum
            else:
                ratio = abs(1.0/float(cur_dfs[_TYP_DF_NM].shape[0]))
            cur_df = cur_dfs[_AST_DF_NM]
            out_dfs["FA"][sector][0] = abs(
                                ratio*cur_df["depreciable_assets_net"][0]
                                )
            out_dfs["INV"][sector][0] = abs(
                                    ratio*cur_df["inventories_net"][0]
                                    )
            out_dfs["LAND"][sector][0] = abs(
                                            ratio*cur_df["land_net"][0]
                                            )
        # Sole Proprietorships:
        sector = _ALL_SECTORS_DICT["SOLE_PROP"]
        if cur_dfs[_INC_DF_NM]["depreciation"][0] != 0:
            ratio = abs(float(cur_dfs[_NFARM_DF_NM]["depreciation_deductions"][0])/
                        cur_dfs[_INC_DF_NM]["depreciation"][0])
        else:
            ratio = 0.0
        cur_df = cur_dfs[_AST_DF_NM]
        out_dfs["FA"][sector][0] = abs(
                                (ratio*
                                cur_df["depreciable_assets_net"][0])+
                                cur_dfs[_FARM_DF_NM]["FA"][0]
                                )
        out_dfs["INV"][sector][0] = abs(
                                (ratio*cur_df["inventories_net"][0])+
                                cur_dfs[_FARM_DF_NM]["Land"][0]
                                )
        out_dfs["LAND"][sector][0] = abs(ratio*cur_df["land_net"][0])
    return asset_tree

