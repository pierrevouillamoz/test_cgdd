import pandas as pd
import re
from numpy import nan

#Extraction functions

def check_url(URL):

    """

    """
    try:
        with open('journal.txt', 'r') as journal:
            journal=journal.read()
    
    except:
        with open('journal.txt', 'w') as journal:
            journal.write("==== URL utilisée(s) ====="+"\n")

    else:
        try:
            journal=journal.split("\n")
            if URL in journal:
                print("=> URL déjà utilisée pour téléchargement")
                return "bad"
            else:
                return "good"
        except:
            
            if URL in journal:
                print("=> URL déjà utilisée pour téléchargement")
                return "bad"
            else:
                
                return "good"

def get_data(URL):
    
    """
    This function extract data of the given URL. Try block allows to test if URL is correct and if data schema is compatible with our project
    """
    try:
        data=pd.read_csv(URL,
                    sep=";",
                    low_memory=False,
                    parse_dates=["journey_start_date","journey_start_datetime","journey_end_datetime","journey_end_date"],
                    date_format='%Y-%m-%d'
                        )
        #as further computation will be made on these columns
        assert "journey_start_country" in data.columns
        assert "journey_end_country" in data.columns
        assert "journey_distance" in data.columns
        assert "journey_duration" in data.columns

    except AssertionError:
        return "=> Le schéma du fichier source n'est pas compatible avec le programme"

    except:
        return "=> L'URL semble inexacte ou le fichier source est corrompu"
    
    
    else:
        #As URL work, it saved on the journal
        with open('journal.txt', 'a') as journal:
            journal.write(URL+"\n")
        return data
    

#Transformation functions

def france_correction(x):
    
    """
    This function revises basic errors of the word "France".
    """
    x=str(x)
    x=x.upper()
    
    common_mistakes = [france.upper() for france in ['Frnace', 'Frace', 'farce', 'Frnce', 'Frane', 'Frnac', 'Frae','Franc', 'Fraance', 'Franca', 'Françe', 'Fance']]
    
    if x in common_mistakes:
        x="FRANCE"
        return x
    else:
        return x

def verification_france(start_country,end_country):
    
    """
    This function compares start versus end country. If journeys starts and/or ends in France, function will return "Journey in France". 
    Else function returns "Not France at all".
    This function has been developed to remove journey done fully outside of France

    """

    start_country=str(start_country)
    end_country=str(end_country)

    start_country=start_country.upper()
    end_country=end_country.upper()

    if (start_country!="FRANCE") and (end_country!="FRANCE"):
        return "Not France at all"
    else:
        return "Journey in France"       
     
def remove_units(x):

    """
    Function to remove unexpected unit for numerical values.
    Function also converts bad record unities to the expected ones.
    """

    km=False
    h=False
    d=False

    if type(x)==str:

        if ("km" in x) or ("kms" in x) or ("kilometers" in x):
            km=True
        if ("h" in x) or ("hour" in x) or ("hours" in x) or ("hrs" in x):
            h=True
        if ("day" in x) or ("days" in x) or ("d" in x):
            d=True
        
        x=re.sub(r'[^\d.,]', '', str(x)).replace(',', '.')
        x=float(x)

        if km==True:
            x=x*1000
        if h==True:
            x=x*60
        if d==True:
            x=x*1440
    else:
        pass

    return x

def zero_to_one(x):
    
    """
    To perform division (avoid 0 value) and to ensure compliance of positive value
    """
    try:
        x=float(x)
        if x<0:
            return -x
        if x==0:
            return 1
        else:
            return x
    except ValueError:   
        #Eliminating not a number values
        return None

def load(data):
    
    """
    This function is the end of ETL process (Load)
    With the try and except block, function check if data should be add to an existing file or to a new file.
    Case of an existing corrupted file is taken into account
    """

    try: 
        journey_per_day=pd.read_csv("journey_per_day.csv",
                                    sep=";",
                                    low_memory=False,
                                    parse_dates=["journey_start_date"],
                                    decimal=',',
                                    )
        
        journey_per_day=journey_per_day.set_index("journey_start_date")
        
        assert "number of journeys estimated (thousand)" in journey_per_day.columns
    
    except AssertionError:
        print("=> Le fichier de l'indicateur semble corrompu\n=> Un nouveau sera créé : journey_per_day.csv")
        data.to_csv("journey_per_day.csv", sep=";",  decimal=',')
    except:
        print("=> Il n'y a pas de fichier indicateur déjà créé\n=> Un nouveau sera créé : journey_per_day.csv")
        data.to_csv("journey_per_day.csv", sep=";",  decimal=',')
    
    else:
        data=pd.concat([journey_per_day,data], axis=0)
        
        #
        
        index_clean=data.index.drop_duplicates(keep='last')
        data=data.loc[index_clean]
        data=data.sort_index()
        
        data.to_csv("journey_per_day.csv", sep=";",  decimal=',')
        print("Succesfull data loading")


