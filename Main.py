# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 09:24:34 2025

@author: shay
test
"""



import os
import pandas as pd
from separate_protein_files import split_protein_data
from add_scores import compute_and_add_scores
from anomaly_selection import filter_anomalous_data
from isomer_handling import handle_isomers
from produce_ml_labels import generate_ml_labels
from add_negatives import add_negative_samples_from_masterlist
from fingerprint_extraction import extract_fingerprints
from column_selection import select_final_columns

def process_csv_files(data_path, masterlist_path, separated_files_dir, output_dir1, output_dir2, MasterList_Information, DesiredColumns):
    """Processes all CSV files through data curation steps."""

    # Step 1: Separate protein-related data and store in subfolders
    for file_name in os.listdir(data_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(data_path, file_name)
            print(f"Processing: {file_name}")

            # Create a subfolder for the separated files
            subfolder = os.path.join(separated_files_dir, os.path.splitext(file_name)[0])
            os.makedirs(subfolder, exist_ok=True)

            # Step 1: Split protein files
            separated_files = split_protein_data(file_path, subfolder)
            
            # Step 2: Compute and Add Scores to all separated files together
            print("\nComputing and Adding Scores to All Separated Files...\n")
            compute_and_add_scores(separated_files)  # Process scores in a batch  


            # Step 3-8: Process each separated file after computing scores
            for sep_file in separated_files:
                sep_file_name = os.path.basename(sep_file)
                print(f"  Processing separated file: {sep_file_name}")
        
                # Load separated file
                df = pd.read_csv(sep_file)
        
                # Step 3: Identify and filter out anomalies
                df = filter_anomalous_data(df,sep_file_name)
        
                # Step 4: Handle isomer-specific corrections
                df = handle_isomers(df,sep_file_name)
        
                # Step 5: Add additional negative samples from master list
                df = add_negative_samples_from_masterlist(df, file_name, masterlist_path,MasterList_Information)
                
                # Step 6: Generate ML labels
                df = generate_ml_labels(df)
        
                # Save curated CSV file (MLReady)
                output_file1 = os.path.join(output_dir1, f"MLReady_{sep_file_name}")
                df.to_csv(output_file1, index=False)
        
                print(f"  Saved intermediate file: {output_file1}")
        
                # Step 7: Extract chemical fingerprints
                #df = extract_fingerprints(df) #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        
        
                # Step 8: RenameColumns
                df = df.rename(columns={"BINARY_LABEL": "SGC_BINARY_LABEL"})
                
                # Step 9: Select final columns
                df = select_final_columns(df, DesiredColumns)                
       
                # Save as CSV
                output_file2_csv = os.path.join(output_dir2, f"MLReadyPlusFPs_{sep_file_name}.csv")
                df.to_csv(output_file2_csv, index=False)
                
                # Save as Parquet
                output_file2_parquet = os.path.join(output_dir2, f"MLReadyPlusFPs_{sep_file_name}.parquet")
                df.to_parquet(output_file2_parquet, index=False)
                
                print(f"Saved CSV: {output_file2_csv}")
                print(f"Saved Parquet: {output_file2_parquet}")

def main(data_path, masterlist_path, separated_files_dir, output_dir1, output_dir2, MasterList_Information, DesiredColumns):
    """Main function to execute the full data curation pipeline."""
    os.makedirs(separated_files_dir, exist_ok=True)
    os.makedirs(output_dir1, exist_ok=True)
    os.makedirs(output_dir2, exist_ok=True)

    process_csv_files(data_path, masterlist_path, separated_files_dir, output_dir1, output_dir2, MasterList_Information, DesiredColumns)

if __name__ == "__main__":
    # Define paths (Modify as needed)
    path=r"D:\0000-UHN\03-DataAndCodes\Data\ASMS\EASMS-7March"
    data_path = os.path.join(path, "RawData0")
    masterlist_path = os.path.join(path, "MasterLists")  
    MasterList_Information = os.path.join(masterlist_path, "MasterList_Information.xlsx")   

    separated_files_dir = os.path.join(path, "Separated_Files")                                 
    output_dir1 = os.path.join(path, "MLReady") 
    output_dir2 = os.path.join(path, "MLReady_Plus_FPs")

   ''' DesiredColumns = ['ASMS_BATCH_NUM',
     'COMPOUND_ID',
     'COMPOUND_FORMULA',
     'SMILES',
     'POOL_NAME',
     'POOL_ID',
     'POOL_SIZE',
     'PROTEIN_NUMBER',
     'TARGET_ID',
     'PROTEIN_ID',
     'PROTEIN_SEQ',
     'PROTEIN_TAG',
     'INCUBATION_VOLUME',
     'PROTEIN_CONC',
     'COMPOUND_CONC',
     'MS_REPRODUCABILITY',
     'POS_INT_REP1',
     'POS_INT_REP2',
     'POS_INT_REP3',
     'TARGET_VALUE',
     'SELECTIVE_VALUE',
     'NTC_VALUE',
     'ENRICHMENT',
     'SELECTIVE_ENRICHMENT',
     'PVALUE',
     'BINARY_LABEL',
     'SPR',
     'KD',
     'ISOMERS',
     'MassSpec_Detected',
     'EASMS_ENRICHMENT',
     'MEAN_NONTARGET_VALUES',
     'LABEL',
     'MW',
     'ALOGP',
     'ECFP4',
     'ECFP6',
     'FCFP4',
     'FCFP6',
     'MACCS',
     'RDK',
     'AVALON',
     'TOPTOR',
     'ATOMPAIR']'''
    
    DesiredColumns = ['ASMS_BATCH_NUM',
     'COMPOUND_ID',
     'COMPOUND_FORMULA',
     'SMILES',
     'POOL_NAME',
     'POOL_ID',
     'POOL_SIZE',
     'PROTEIN_NUMBER',
     'TARGET_ID',
     'PROTEIN_ID',
     'PROTEIN_SEQ',
     'PROTEIN_TAG',
     'INCUBATION_VOLUME',
     'PROTEIN_CONC',
     'COMPOUND_CONC',
     'MS_REPRODUCABILITY',
     'POS_INT_REP1',
     'POS_INT_REP2',
     'POS_INT_REP3',
     'TARGET_VALUE',
     'SELECTIVE_VALUE',
     'NTC_VALUE',
     'ENRICHMENT',
     'SELECTIVE_ENRICHMENT',
     'PVALUE',
     'BINARY_LABEL',
     'SPR',
     'KD',
     'ISOMERS',
     'MassSpec_Detected',
     'EASMS_ENRICHMENT',
     'MEAN_NONTARGET_VALUES',
     'LABEL',
     'MW',
     'ALOGP',
     'ECFP4',
     'ECFP6',
     'FCFP4',
     'FCFP6',
     'MACCS',
     'RDK',
     'AVALON',
     'TOPTOR',
     'ATOMPAIR']

    main(data_path, masterlist_path, separated_files_dir, output_dir1, output_dir2, MasterList_Information, DesiredColumns)
