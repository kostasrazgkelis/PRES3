from os import name
from flask import json
from flask.json import jsonify
from flask.wrappers import Response
import pandas as pd
import hashlib
import jellyfish
from sqlite3 import connect
import flask
import random
import os
import pyspark

app = flask.Flask(__name__)
HOST = '0.0.0.0'
PORT = 2000


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


@app.route('/main/', methods=['GET'])
def new():

    Dictionary ={   'TP': 0.1231231231923492835823949235829348175681398318283134918985194192481851283123123123192349283582394923582934817568139831828313491898519419248185128312312312319234928358239492358293481756813983182831349189851941924818512831231231231923492835823949235829348175681398318283134918985194192481851283, 
                    'FP': 0.1231231231923492835823949235829348175681398318283134918985194192481851283123123123192349283582394923582934817568139831828313491898519419248185128312312312319234928358239492358293481756813983182831349189851941924818512831231231231923492835823949235829348175681398318283134918985194192481851283,
                    'FN': 0.1231231231923492835823949235829348175681398318283134918985194192481851283123123123192349283582394923582934817568139831828313491898519419248185128312312312319234928358239492358293481756813983182831349189851941924818512831231231231923492835823949235829348175681398318283134918985194192481851283}

    json_string = json.dumps(Dictionary)

    return Response(json_string)

  




def main():

    noise = 100
    data = pd.read_csv("C:/Users/Kostas Razg/Desktop/Πτυχιακή/A_1k_names_separated.csv", header=0, names=['id', 'name', 'surname'] , encoding='utf-8')
    conn = connect(':memory:')


    column = data.columns[2]
    
    fake_soundex_values = create_fake_soundex_values(noise, data.shape[0], data.columns)

    for column in data:
        if column != 'id':
            try:
                data[column] = data[column].apply(lambda x: jellyfish.soundex(x)).apply(lambda x: hashlib.sha256( x.encode()).hexdigest() )
                fake_soundex_values[column] = fake_soundex_values[column].apply(lambda x: hashlib.sha256( x.encode()).hexdigest() )
            except Exception as e:
                print(f"There was an error! {e}")
            
    merged_data = pd.concat([data, fake_soundex_values], axis=0).sort_values(by=column)
    print(merged_data)

    merged_data.to_sql('data', conn)

    results = pd.read_sql(' SELECT count(*) as result\
                        FROM data\
                        ', conn)

    print(results['result'][0])

    # result = hashed.append(fake_soundex)
    
    # result.index.drop
    # print(result)

if __name__ == '__main__':
    ENVIRONMENT_DEBUG = os.environ.get("DEBUG", True)
    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)