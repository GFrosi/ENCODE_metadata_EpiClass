import pandas as pd
import numpy as np
import sys
import json
import glob
import os
import argparse
from pathlib import Path




def write_merge_json(out_name, list_dset):
    """Write content to json path"""
    
    with open(out_name, 'w') as f:
        json.dump({"datasets":list_dset}, f)



def merge_json(path):

    list_dset = []
    for f in glob.glob(os.path.join(path,"*.json")):
        print(f)
        file_n = json.load(open(f))
        for dset in file_n['datasets']:

            list_dset.append(dset)
    
    print(len(list_dset))
    return list_dset



def create_json(df_merge, out_name):

    df_merge = df_merge.fillna('')
    json_df = df_merge.to_dict(orient='records')

    with open(out_name, 'w') as f:
        json.dump({"datasets":json_df}, f)



def merge_enco_files_control(df_metadata_merge, df_experiment_enc):

    #manipulating columns to merge dfs
    df_experiment_merge = manipulate_cols_to_merge(df_experiment_enc)
    

    #first merge: original metadata encode + experiment_enc
    df_merge_enc = df_metadata_merge.merge(df_experiment_merge, how='left', left_on='experiment_accession', right_on='accession')
    print('first merge: original metadata encode + experiment_enc:', str(len(df_merge_enc)))

    return df_merge_enc



def merge_enco_files(df_metadata_enc, df_experiment_enc): #if you need geo, pass df_geo

    #manipulating columns to merge dfs
    df_metadata_merge = metadata_create_new_col_control(df_metadata_enc)
    df_experiment_merge = manipulate_cols_to_merge(df_experiment_enc)
    

    #first merge: original metadata encode + experiment_enc
    df_merge_enc = df_metadata_merge.merge(df_experiment_merge, how='left', left_on='experiment_accession', right_on='accession')
    print('first merge: original metadata encode + experiment_enc:', str(len(df_merge_enc)))

    return df_merge_enc



def manipulate_cols_to_merge(df_experiment):
    """Receives EpiAtlas_Metadata and Experiment_ENCODE dfs.
    Return a EpiAtlas_Metadata containing a new column equivalent
    to Files column in Experiment_ENCODE. The column was created
    by matching a ENC ID from input column available in EpiAtlas_
    Metadata"""


    df_experiment['files_replace'] = df_experiment.loc[:,'files'].str.replace('/files/', '') #Creating a new Files col without '/' and files string
    df_experiment['files_replace'] = df_experiment.loc[:,'files_replace'].str.replace('/', '')#Creating a new Files col without '/' and files string

    return df_experiment


def metadata_create_new_col_control(df_metadata_enc):

    #CORE
    df_metadata_enc['assay_epiclass'] = 'input' #done

    #create track_type column
    df_metadata_enc['track_type'] = 'raw'

    #create uuid by combining Experiment accession; Experiment target and File analysis title
    df_metadata_enc['uuid'] = df_metadata_enc.loc[:,'experiment_accession'].map(str) + '-' + df_metadata_enc['assay_epiclass'].map(str) + '-' +  df_metadata_enc['file_analysis_title']

    #renaming columns
    df_metadata_enc.rename(columns={'md5sum': 'md5sum_encode', 'file_accession': 'md5sum'}, inplace=True)

    return df_metadata_enc



def metadata_create_new_col(df_metadata_enc):

    #creating new columns
    df_metadata_enc['assay_target'] = df_metadata_enc.loc[:,'experiment_target'].str.split('-').str.get(0) #done

    #NON_CORE
    #Updated from experiment_type to assay_epiclass. Also, others -> non-core (for non-core)
    # df_metadata_enc_filter['assay_epiclass'] = np.where((df_metadata_enc_filter['Assay'] == 'CTCF'), 'CTCF', 'non-core')

    #CORE
    df_metadata_enc['assay_epiclass'] = df_metadata_enc.loc[:,'assay'].str.lower()

    #create track_type column
    df_metadata_enc['track_type'] = 'raw'

    #create uuid by combining Experiment accession; Experiment target and File analysis title
    df_metadata_enc['uuid'] = df_metadata_enc.loc[:,'experiment_accession'].map(str) + '-' + df_metadata_enc['Experiment target'].map(str) + '-' +  df_metadata_enc['File analysis title']

    #renaming columns
    df_metadata_enc.rename(columns={'md5sum': 'md5sum_encode', 'file_accession': 'md5sum'}, inplace=True)

    return df_metadata_enc


def stand_columns(df):

    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('\(s\)', 's')

    return df



def main():

    print('ENCODE approach...')
    df_metadata_enc = pd.read_csv(args.metadata_encode, sep="\t") #metadata_ENCODE_6hist_20221202_files.tsv
    df_experiment_enc = pd.read_csv(args.experiment_report, sep="\t") #experiment_report_2022_12_21_16h_21m_noqueryheader.tsv
    
    #standardizing columns (lower case and no space)
    df_metadata_enc_stand = stand_columns(df_metadata_enc)
    df_experiment_enc_stand = stand_columns(df_experiment_enc)

    if args.control_assay:
        print('control mode')
        df_metadata_merge = metadata_create_new_col_control(df_metadata_enc_stand)
        df_merge = merge_enco_files_control(df_metadata_merge,df_experiment_enc_stand)
        df_merge.to_csv('ENCODE_control_metadata_2023_track_assay_epiclass_uuid.tsv', sep='\t', index=False)
        create_json(df_merge, args.output)
    
    else:
        print('Targets mode')
        df_merge = merge_enco_files(df_metadata_enc_stand,df_experiment_enc_stand)
        df_merge.to_csv('ENCODE_metadata_2023_track_assay_epiclass_uuid.tsv', sep='\t', index=False)
        create_json(df_merge, "ENCODE_track_assay_epiclass_uuid.json")

    # # list_dset = merge_json(path) 
    # # write_merge_json("ENCODE_core_and_noncore_2023_track_assay_epiclass_uuid.json", list_dset)



if __name__ == "__main__":


    parser = argparse.ArgumentParser(
        description="A script to create a dataframe and json files from ENCODE metadata")

    parser.add_argument('-m', '--metadata-encode', action="store",
                        type=Path,
                        help='Path to metadata ENCODE csv file.',
                        required=True)

    parser.add_argument('-e', '--experiment-report', action="store",
                        type=Path,
                        help='Path to experiment_report ENCODE csv file.',
                        required=True)

    parser.add_argument('-c', '--control-assay',
                        help='Assay_epiclass category will be created from assay col instead experiment_target col',
                        type=str,
                        default=None,
                        required=False
    )

    parser.add_argument('-o', '--output',
                        help='Output json file name',
                        type=Path,
                        required=True

    )


    args = parser.parse_args()  
    
    
    main()
