import pandas as pd
import utils

#URL of data. 
#URL="https://www.data.gouv.fr/fr/datasets/r/a355ab6a-e131-406d-bf21-25750b6c2f3e"
def ETL(URL):

    print("===== Processus ETL =====\nFile : {}".format(URL))

    for step in ["Extraction","Transformation","Load"]:

        if step=='Extraction':
            #Extraction data and charged as a dataframe
            print("Extracting data (up to 1 min)")

            URL_checked=utils.check_url(URL)
            if URL_checked=="bad":
                print("processus ETL interrompu\n")
                break
            elif URL_checked=="good":
                pass

            data=utils.get_data(URL)

            if isinstance(data, pd.DataFrame):
                print("Succesful extraction")

            elif isinstance(data,str):
                print(data)
                print("processus ETL interrompu\n")
                break 
            
            else:           
                print("processus ETL interrompu\n")
                break 

        if step=='Transformation':
            
            print("Transforming data")
            
            #to ensure this columun holds a date object
            data["journey_start_date"]=pd.to_datetime(data["journey_start_date"], errors='coerce', dayfirst=True)
            nb_before=len(data)

            #to delete NaT on journey_start_column
            data=data.dropna(subset=["journey_start_date"])
            nb_after=len(data)

            #to compute le lost of data in percentage. Threeshold is 5%
            pct=(nb_before-nb_after)/nb_before
            if pct>0:
                print("Le traitement des dates a conduit à une perte du volume de donnée d\'entrée de {} % ".format(pct*100))

            if pct>=0.05:
                print("processus ETL interrompu\n")
                break
            
            #eliminating journey_start_date outside the current month

            #We assume main dates fit with the expected month ans mistake are few
            month=data["journey_start_date"].mean().month
            year=data["journey_start_date"].mean().year

            upper_date_limit=pd.Timestamp(year=year, month=(month+1), day=1)
            lower_date_limit=pd.Timestamp(year=year, month=(month), day=1)

            #data filtration on date
            data=data[data["journey_start_date"]>=lower_date_limit]
            data=data[data["journey_start_date"]<upper_date_limit]

            #Correction made on the word France to ensure accurancy 
            data["journey_start_country"]=data["journey_start_country"].apply(utils.france_correction)
            data["journey_end_country"]=data["journey_end_country"].apply(utils.france_correction)

            #If journey is fulled outside France, rows will be eliminated   
            data["check_country"]=data[["journey_start_country","journey_end_country"]].apply(lambda x :utils.verification_france(x["journey_start_country"],x["journey_end_country"]), axis=1)
            data=data[data["check_country"]!="Not France at all"]


            #Correction on units
            data["journey_distance"]=data["journey_distance"].apply(utils.remove_units)
            data["journey_duration"]=data["journey_duration"].apply(utils.remove_units)
            #Average speed is computed. Although it is not here a filter criterion, it could be used to remove abnormal value for mean calculation
            data["journey_duration"]=data["journey_duration"].apply(utils.zero_to_one)
            data["journey_average_speed"]=(data["journey_distance"]/1000)/(data["journey_duration"]/60)


            #Finally, data are group by journey_start_date
            data=data[["journey_start_date","journey_id"]].groupby(by=["journey_start_date"]).count()/1000*25
            data=data.rename(columns={"journey_id":"number of journeys estimated (thousand)"})

            print("Succesful transformation")
            
        if step=="Load":

            print("Loading data")
            #Results are saved into a new csv file or add to an existant one
            utils.load(data)
            print("Succesful loading")
            print("Processus ETL terminé\n")


    