from os import name
import pandas as pd
import hashlib
import jellyfish

import random

def create_alp():  
    return str(chr(random.randrange(65,90))) + str(chr(random.randrange(48,54))) + str(chr(random.randrange(48,54))) + str(chr(random.randrange(48,54)))

def create_noise(noise, size, name):
    return pd.Series([create_alp() for _ in range(int(noise * size / 100))], name=name)

def create_fake_index(noise,size, name):
    return pd.Series(['Fake Index' for _ in range(int(noise * size / 100))], name=name)

def create_fake_soundex_values(noise, size, columns):
    a = create_noise(noise, size, columns[1])
    b = create_noise(noise, size, columns[2])
    fake_index = create_fake_index(noise, size, columns[0])

    return pd.DataFrame(pd.concat([fake_index, a, b], axis=1))



def main():

    noise = 100
    data = pd.read_csv("C:/Users/Kostas Razg/Desktop/Πτυχιακή/A_1k_names_separated.csv", header=0, names=['id', 'name', 'surname'] , encoding='utf-8')

    column = data.columns[2]
    
    fake_soundex_values = create_fake_soundex_values(noise, data.shape[0], data.columns)

    for column in data:
        if column != 'id':
            try:
                data[column] = data[column].apply(lambda x: hashlib.sha256( x.encode()).hexdigest() )
                fake_soundex_values[column] = fake_soundex_values[column].apply(lambda x: hashlib.sha256( x.encode()).hexdigest() )
            except Exception as e:
                print(f"There was an error! {e}")
            
    merged_data = pd.concat([data, fake_soundex_values], axis=0).sort_values(by=column)
    print(merged_data)

    # result = hashed.append(fake_soundex)
    
    # result.index.drop
    # print(result)

if __name__ == "__main__":
    main()      