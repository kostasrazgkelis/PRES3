from flask import Flask, send_file
import os
from flask.wrappers import Response
import jellyfish
import pandas as pd
import random

HOST = '0.0.0.0'
PORT = '9300'
NAME_OF_CLUSTER = "Cluster_B"
path = f"C:/Users/Kostas Razg/Desktop/Πτυχιακή/MyThesis/myThesis/"

app = Flask(__name__)



def create_alp():  
    return str(chr(random.randrange(65,90))) + str(chr(random.randrange(65,90))) + str(chr(random.randrange(65,90))) + str(chr(random.randrange(65,90)))

@app.route('/')
def get():
    return f'{NAME_OF_CLUSTER}', 200


@app.route("/take_data/<noise>", methods=["GET"])
def post(noise):

    try:
        noise = int(noise)

    except ValueError:
            return Response('<html><head><h1 style="background-color:powderblue;">There was an error!</h1></head></html>', 400)
    else:
        print(f"{noise}")
        if 0 <= noise <= 100:
            
            print(f"{NAME_OF_CLUSTER}- Data is being send")

            try:
                df1 = pd.read_csv("/var/lib/data/B_1k_names_separated.csv", header=0, names=[3,4,5])

                column_1 = df1[0].apply(lambda x: jellyfish.soundex(x))
                column_2 = df1[1].apply(lambda x: jellyfish.soundex(x))
                column_3 = df1[2].apply(lambda x: jellyfish.soundex(x))
                

            except Exception as e:
                return Response('<html><head><h1 style="background-color:powderblue;">There was an error!</h1></head></html>', 400)
            else:

                data_list = list()
                for _ in range(int(noise * df1.shape[0] / 100)):
                    data_list.append( [jellyfish.soundex( create_alp()) for _ in range(df1.shape[1]) ] )

                df2 = pd.DataFrame(data_list, columns=[0,1,2])
                df2.to_csv("/var/lib/data/noise.csv", encoding='utf-8', index=False)


                # Merge both datasets
                result = pd.concat([column_1, column_2, column_3], axis=1)

                result = pd.concat([result,df2], ignore_index=True, axis=0)

                #result = pd.util.hash_pandas_object(result, index=True, encoding='utf8', hash_key='0123456789123456', categorize=True).sort_values(by=2)
  
                result.to_csv('/var/lib/data/joined_data.csv', encoding='utf-8', index=False)
                     
                print(f"{NAME_OF_CLUSTER}- the download has finished")

                return send_file('/var/lib/data/joined_data.csv',
                            mimetype='text/csv',
                            attachment_filename='b_cluster_data.csv',
                            as_attachment=True)

        else:
            return Response('<html><head><h1 style="background-color:red;">Error: You need an intenger 0-100. </h1></head></html>', 200)
        

if __name__ == '__main__':
    ENVIRONMENT_DEBUG = os.environ.get("DEBUG", True)
    app.run(host=HOST, port=PORT, debug=ENVIRONMENT_DEBUG)