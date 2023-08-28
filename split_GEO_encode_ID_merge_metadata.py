import pandas as pd
import sys




def merge_enco_geo_files(df_metadata_enc,df_experiment_enc, df_geo, df_epiatlas, col): #if you need geo, pass df_geo

    #manipulating columns to merge dfs
    df_epiatlas_merge, df_experiment_merge = manipulate_cols_to_merge(df_epiatlas, df_experiment_enc, col)
    # print(df_epiatlas_merge)
    # print(df_experiment_merge)


    #first merge: original metadata encode + experiment_enc
    df_merge_enc = df_metadata_enc.merge(df_experiment_merge, how='left', left_on='Experiment accession', right_on='Accession')
    print('first merge: original metadata encode + experiment_enc:', str(len(df_merge_enc)))

    #second merge: merge_1 + geo enc (our extraction)
    df_merge_geo = pd.merge(df_merge_enc,df_geo[['ENCODE_GSM', 'ENCODE_GSE']], how='left', left_on='Experiment accession', right_on='ENCODE_GSE')
    print('second merge: merge_1 + geo enc (our extraction):', str(len(df_merge_geo)))


    #third merge: merge_2 + epiatlas metadata combined with v1 IHEC epimap
    df_merge_epiatlas = df_merge_geo.merge(df_epiatlas_merge, how='left', left_on='Files_replace', right_on='new_enc_1')
    # df_merge_epiatlas = df_merge_enc.merge(df_epiatlas_merge, how='left', left_on='Files_replace', right_on='new_enc_1')

    print('third merge: merge_2 + epiatlas metadata combined with v1.1 IHEC epimap:', str(len(df_merge_epiatlas)))

    df_merge_epiatlas.to_csv('ENCODE_metadata_ctl_2023_plus_GEO_EpiAtlasv11.tsv', index=False, sep="\t")    



def manipulate_cols_to_merge(df_epiatlas, df_experiment, col):
    """Receives EpiAtlas_Metadata and Experiment_ENCODE dfs.Also
    receives a col name.

    encode_core: you should pass col: inputs
    encode_ctl: you should pass col: inputs_ctl

    Return a EpiAtlas_Metadata containing a new column equivalent
    to Files column in Experiment_ENCODE. The column was created
    by matching a ENC ID from input column available in EpiAtlas_
    Metadata"""

    df_epiatlas['new_enc'] = df_epiatlas[col].str.split(';').str.get(0) #getting first ele from input col in EpiAtlas Metadata (update - input_ctrls to input)
    df_experiment['Files_replace'] = df_experiment['Files'].str.replace('/files/', '') #Creating a new Files col without '/' and files string
    df_experiment['Files_replace'] = df_experiment['Files_replace'].str.replace('/', '')#Creating a new Files col without '/' and files string

    # #match values from epiatlas into encode
    for ind1 in df_epiatlas.index: #matching new_enc value from EpiAtlas metadata in Files_replace column from Experiment_ENCODE 

        try: 
            df_epiatlas.loc[ind1, 'new_enc_1'] = ', '.join(list(df_experiment[df_experiment['Files_replace'].str.contains(df_epiatlas['new_enc'][ind1])]['Files_replace']))
            # print(df_experiment['new_enc_1'])

        except:
            print('Nothing in index:',ind1)
            # continue
    
 
    return df_epiatlas, df_experiment



def create_enc_cols(df):

    # df_geo = df.iloc[:,0:22].copy() #necessary to use copy 
    df_geo = df.iloc[:].copy()
    # print(df_geo)

    #creating ENC column from GSM_title
    df_geo['ENCODE_GSM'] = df_geo['Gsm-title'].str.extract('(\(ENC.*\))', expand=True)
    df_geo['ENCODE_GSM'] = df_geo['ENCODE_GSM'].str.strip('()') #remove parenthesis

    #creating ENC column from GSE_title
    df_geo['ENCODE_GSE'] = df_geo['Gse-title'].str.extract('(\(ENC.*\))', expand=True)
    df_geo['ENCODE_GSE'] = df_geo['ENCODE_GSE'].str.strip('()') #remove parenthesis

    return df_geo



def main():

    df = pd.read_csv(sys.argv[1], sep="\t") #df Compiled_dbs_2022_outer_92861_2022_12_01_filled_gsm_xml.tsv / Histone_DBs
    df_metadata_enc = pd.read_csv(sys.argv[2], sep="\t") #metadata_ENCODE_6hist_20221202_files.tsv
    df_experiment_enc = pd.read_csv(sys.argv[3], sep="\t") #experiment_report_2022_12_21_16h_21m.tsv
    df_epiatlas = pd.read_csv(sys.argv[4]) #/Users/gfrosi/scratch/EpiAtlas-QC/script_get_updates_epirrversion/epiatlas_dfreze_plusv1_UPDATED.csv
    # df_out_geo = sys.argv[5]
   
    #create enc cols based on GEO metadata (GSM_title and GSE_title)
    df_geo = create_enc_cols(df)
    # df_geo.to_csv(df_out_geo, index=False, sep="\t")
    # sys.exit()
    
    merge_enco_geo_files(df_metadata_enc,df_experiment_enc, df_geo, df_epiatlas, 'inputs_ctl')
    # merge_enco_geo_files(df_metadata_enc,df_experiment_enc, df_epiatlas)

    


if __name__ == "__main__":


    main()