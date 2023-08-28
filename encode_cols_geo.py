import pandas as pd
import sys



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

    df_geo[['ENCODE_GSE', 'ENCODE_GSM']] = df_geo[['ENCODE_GSE', 'ENCODE_GSM']].fillna('----')

    return df_geo




def main():

    print('Starting ENCODE-GEO script')
    df = pd.read_csv(sys.argv[1], sep="\t") #Histones_basedDBs_57566_2023_08_03_consensus.tsv - after main_stand_dbs.py and compare_dbs (consensus columns)
    df_out_geo = sys.argv[2]
    
    print('Saving Histone_dbs_consensus_ENCODE.csv')
    df_geo = create_enc_cols(df)
    df_geo.to_csv(df_out_geo, index=False, sep="\t")
    print('Files succesfully saved!')

 
if __name__ == "__main__":



    main()