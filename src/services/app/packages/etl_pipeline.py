import pandas as pd
import packages.transformation_functions as tf
import jellyfish
import hashlib

correct_path = f'/var/lib/data/datafile.csv'


class ETLModel:
    """
        This is the ETL class model that will be used in backend for our application.
        The ETL pipeline is rensponsible for the excraction, transformation and loading
        the data.
    """

    def __init__(self, path: str, columns: list, matching_field: str, noise: int):
        self.matching_field = matching_field
        self.noise = noise
        self.path = path
        self.columns = columns

    def extract_data(self) -> pd.DataFrame:
        """ This function exctracts the data from a source location. (e.g csv file)

        Returns:
            pd.DataFrame: A pandas dataframe
        """
        self.dataframe = pd.read_csv(self.path, header=0).astype('string')

    def transform_data(self) -> pd.DataFrame:
        """ This function take the exctracted dataframe and starts transforming the data (cleans the data)

        Args:
            matching_field (str): the column which will be used to make the join operation to find our matches
            noise (int): The percentage of the noise we are going to use in our dataset (e.g. 10%, 20%)
            dataframe (pd.DataFrame): The dataframe with the data

        Returns:
            pd.DataFrame: The transformed data
        """
        columns_we_care = self.columns
        columns_we_care.remove(self.matching_field)

        fake_soundex_values = tf.create_fake_soundex_values(self.noise, self.dataframe, self.matching_field)

        for column in columns_we_care:
            # Applies to the data the jellyfish-soundex function and also encodes them with the SHA256 encryption
            self.dataframe[column] = self.dataframe[column] \
                .apply(lambda x: jellyfish.soundex(str(x))) \
                .apply(lambda x: hashlib.sha256(x.encode()).hexdigest())

            # Creates fake soundex values
            fake_soundex_values[column] = fake_soundex_values[column].apply(
                lambda x: hashlib.sha256(x.encode()).hexdigest())

        # We merge all the fake soundex data with the correct
        merged_data = pd.concat([self.dataframe, fake_soundex_values], axis=0).sort_values(by='first_name')
        merged_data.reset_index(drop=True, inplace=True)

        self.dataframe = merged_data

    def load_data(self):
        """ Loads the data to a dumpfile (csv file) and stores them in the storage inside the container

        Args:
            dataframe (pd.DataFrame): _description_
        """
        self.dataframe.to_csv('/var/lib/data/hidden_information.csv', encoding='utf-8', header=True, index=False)

    def start_etl(self):
        """ Start the etl pipeline
        """
        self.extract_data()
        self.transform_data()
        self.load_data()