import pandas as pd
import sys

df_geo = pd.read_csv(sys.argv[1])
df_enc = pd.read_csv(sys.argv[2], sep="\t")


df_merge_geo = pd.merge(df_enc,df_geo[['ENCODE_GSE']], how='left', left_on='experiment_accession', right_on='ENCODE_GSE')
df_final = df_merge_geo.drop_duplicates() #encode GSM does not represent the reality

df_final.to_csv("ENCODE_core_ctl_metadata_plus_GEO_nodup.csv", index=False)