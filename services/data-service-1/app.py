from flask import Flask, json, send_file , request, jsonify
import os
import flask
from flask.wrappers import Response
import jellyfish
import pandas as pd
import random
import hashlib
from sqlite3 import connect
import requests
import numpy as np

HOST = '0.0.0.0'
PORT = '9200'
NAME_OF_CLUSTER = "Cluster_A"
test_path = f"C:/Users/Kostas Razg/Desktop/Πτυχιακή/MyThesis/myThesis/A_1k_names_separated.csv"
correct_path = f'/var/lib/data/A_1k_names_separated.csv'

app = Flask(__name__)



def create_alp():  
    return str(chr(random.randrange(65,90))) + str(chr(random.randrange(48,54))) + str(chr(random.randrange(48,54))) + str(chr(random.randrange(48,54)))

def create_noise(noise, size, name):
    return pd.Series([create_alp() for _ in range(int(noise * size / 100))], dtype = 'object', name=name)

def create_fake_index(noise,size, name):
    return pd.Series(['Fake Index' for _ in range(int(noise * size / 100))], dtype = 'object', name=name)

def create_fake_soundex_values(noise, size, columns):
    a = create_noise(noise, size, columns[1])
    b = create_noise(noise, size, columns[2])
    fake_index = create_fake_index(noise, size, columns[0])

    return pd.DataFrame(pd.concat([fake_index, a, b], axis=1))

def precision():

    try:
        a_joined_data    = pd.read_csv('/var/lib/data/final_data.csv')
        b_joined_data    = pd.read_csv('/var/lib/data/data_send_from_bob.csv')

    except Exception as e:
        return f"There was an error opening the files! reason: {e}" 
    else:
        conn = connect(':memory:')

        a_joined_data.to_sql('a_joined_data', conn)
        b_joined_data.to_sql('b_joined_data', conn)


        results = pd.read_sql(' SELECT count(*) as false_positive\
                                FROM a_joined_data, b_joined_data\
                                WHERE a_joined_data.[id] == b_joined_data.[id] and (a_joined_data.[surname] != b_joined_data.[surname] or a_joined_data.[name] != b_joined_data.[name] ) ', conn)

        FP = results['false_positive'][0]

        results = pd.read_sql(' SELECT count(*) as true_positive\
                                FROM a_joined_data , b_joined_data \
                                WHERE a_joined_data.[id] == b_joined_data.[id] and (a_joined_data.[surname] == b_joined_data.[surname] and a_joined_data.[name] == b_joined_data.[name] ) ', conn)

        TP = results['true_positive'][0]

        results = pd.read_sql(' SELECT count(*) as false_negative\
                                FROM a_joined_data , b_joined_data \
                                WHERE a_joined_data.[id] != b_joined_data.[id] and (a_joined_data.[surname] == b_joined_data.[surname] and a_joined_data.[name] == b_joined_data.[name] ) ', conn)

        FN = results['false_negative'][0]


        Dictionary ={   'TP': str(TP), 
                        'FP': str(FP),
                        'FN': str(FN)}
 
        json_string = json.dumps(Dictionary)

        return Response(json_string)



@app.route('/return_statistics/', methods=['GET'])
def return_call():
    return precision() 


@app.route('/')
def get():
    return Response(200)

@app.route("/take_data/", methods=["POST"])
@app.route("/take_data/<matching_field>/", methods=['GET'])
@app.route("/take_data/<matching_field>/<int:noise>", methods=["GET"])
def post(matching_field=None, noise=None):

    if request.method == 'GET':

        try:
                data_frame = pd.read_csv(correct_path, header=0, names=['id','surname','name'])

        except Exception as e:
                return Response('<html><head><h1 style="background-color:powderblue;">There is not such file</h1></head></html>', 400)

        else:        

            if 0 <= noise <= 1000 and matching_field in data_frame.columns:
                
                fake_soundex_values = create_fake_soundex_values(noise, data_frame.shape[0], data_frame.columns)
                try: 
                    for column in data_frame:
                        if column != 'id':

                            #Applies to the data the jellyfish-soundex function and also encodes them with the SHA256 encryption
                            data_frame[column] = data_frame[column].apply(lambda x: jellyfish.soundex(x)).apply(lambda x: hashlib.sha256( x.encode()).hexdigest() )

                            #Creates fake soundex values
                            fake_soundex_values[column] = fake_soundex_values[column].apply(lambda x: hashlib.sha256( x.encode()).hexdigest() )


                except Exception as e:
                    return Response(f'<html><head><h1 style="background-color:powderblue;">There was an error! {e} </h1></head></html>', 400)

                else:
                    
                    #We merge all the fake soundex data with the correct 
                    merged_data = pd.concat([data_frame, fake_soundex_values], axis=0).sort_values(by=column)
                    merged_data.to_csv('/var/lib/data/complete_data.csv', encoding='utf-8', header=True, index=False)

                    merged_data.drop('id', axis=1)
                    merged_data.to_csv('/var/lib/data/hidden_information.csv', encoding='utf-8', header=True, index=False)
                    #We send back the final merged file
                    return send_file(f'/var/lib/data/hidden_information.csv',
                                mimetype='text/csv',
                                attachment_filename='a_cluster_data.csv',
                                as_attachment=True)

            else:
                return Response('<html><head><h1 style="background-color:powderblue;">Noise must be 0-100 or matching field doesnt exist</h1></head></html>', 400)


    elif request.method == 'POST':

        try:
            downloaded_data = pd.read_csv(request.files['file'])
        except FileExistsError as e:
            return Response(f"We could not find the file!")
        else:
            downloaded_data.to_csv('/var/lib/data/joined_data.csv', encoding='utf-8', index=False)  
            return Response(requests.get(f'http://cluster-b:9300//get_initial_data/'))




@app.route("/send_data_to_bob/", methods=['GET'])
def send_indexed_data():

    #we need to open the files with all the data we have created so far
    try:
        initial_data   = pd.read_csv('/var/lib/data/A_1k_names_separated.csv', header=0 , names=['id','surname','name']).astype(str)
        complete_data  = pd.read_csv('/var/lib/data/complete_data.csv', header=0).astype(str)
        joined_data    = pd.read_csv('/var/lib/data/joined_data.csv', header=0).astype(str)

    except Exception as e:
        return f"There was an error opening any of the files. {e} "
    else:
        
        #we need to start a SQL query
        conn = connect(':memory:')

        #we need to initizilaze our tables
        complete_data.to_sql('complete_data', conn)
        initial_data.to_sql('initial_data', conn)
        joined_data.to_sql('joined_data', conn)
        


        #we get the hash values from the 
        results = pd.read_sql(' SELECT complete_data.[id] \
                                FROM complete_data , joined_data \
                                WHERE joined_data.[surname] == complete_data.[surname] and joined_data.[name] == complete_data.[name] and complete_data.[id] != "Fake Index" ', conn)

        results.to_sql('matched_entities', conn)
        
        #we finally get the rest of the matched data and we prepare to send these information to Alice
        results = pd.read_sql(' SELECT initial_data.[id], initial_data.[name], initial_data.[surname]\
                                FROM initial_data, matched_entities \
                                WHERE initial_data.[id] == matched_entities.[id] ', conn)

        results.reset_index(drop=True, inplace=True)
        results.to_csv('/var/lib/data/final_data.csv', encoding="utf-8", index=False, columns=['id','name','surname'])

        myfiles = {'file': open('/var/lib/data/final_data.csv' ,'rb')}
        requests.post(f'http://cluster-b:9300//accept_data_from_alice/', files = myfiles)

        return Response('Everything was completed succesfully', 200)


@app.route('/accept_data_from_bob/', methods=['POST'])
def get_data_from_alice():
        #accept the final data from alice
        accepted_data_from_alice = pd.read_csv(request.files['file'])
        accepted_data_from_alice.to_csv('/var/lib/data/data_send_from_bob.csv', encoding='utf-8', index=False)
        return Response('The data has been downloaded succesfully')


@app.route('/send_initial_data/', methods=['GET'])
def send_initial_data():
    try:
        with open('/var/lib/data/joined_data.csv', 'r'):
            pass
    except FileNotFoundError:
        return Response("File does not exists")
    else:
        return send_file(f'/var/lib/data/joined_data.csv',
                    mimetype='text/csv',
                    attachment_filename='A_joined_data.csv',
                    as_attachment=True)

@app.route('/get_initial_data/', methods=['GET'])
def get_initial_data():
    request = requests.get(f"http://cluster-b:9300//send_initial_data")
    url_content = request.content
    with open("/var/lib/data/B_joined_data.csv", 'wb') as file:
        file.write(url_content)

    return Response('We got the data from the Bob')
    

if __name__ == '__main__':
    ENVIRONMENT_DEBUG = os.environ.get("DEBUG", True)
    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)