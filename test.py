import pandas as pd
import csv
import jellyfish
import hashlib

def main():

    df = pd.read_csv('C:/Users/Kostas Razg/Desktop/Πτυχιακή/MyThesis/myThesis/services/data-service-1/book_chapter_table_25p_200k_A.csv', delimiter=';').astype('string')
    
    df['last_name'] = df['last_name'].apply(lambda x: jellyfish.soundex(str(x))).apply(lambda x: hashlib.sha256( x.encode()).hexdigest() )
    print(df['last_name'])

if __name__ == '__main__':
    main()