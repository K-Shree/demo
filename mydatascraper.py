import pandas as pd
import requests
import sys
from bs4 import BeautifulSoup
from time import sleep
from random import randint
import re
import pickle

agent = {'user-agent' : 'my-app/0.0.1'}

base = "https://schools.org.in"
page = requests.get(base, headers=agent)
parser = BeautifulSoup(page.content, "html.parser")

content = parser.select(".card-columns a")

def get_data(extn) :
    data = requests.get(base+"/"+extn, headers=agent)
    parser = BeautifulSoup(data.content, "html.parser")
    lists = parser.select(".table-striped td a")
    new_extn = []
    for i in range(len(lists)) :    
        new_extn.append(lists[i].get('href'))
    return new_extn

def get_schooldata(extn) :
    data = requests.get(base+"/"+extn, headers=agent)
    parser = BeautifulSoup(data.content, "html.parser")
    lists = parser.select(".list-group-item")
    name = parser.select(".my-3 h2")
    all_fields = [name[0].text]
    for i in range(len(lists)) :    
        all_fields.append(lists[i].text)
    return all_fields

def get_new_df() :
    cols = ['School Name', 'Instruction Medium', 'Male Teachers', 'Pre Primary Sectin Avilable',
     'Board for Class','School Type','Classes','Female Teacher','Pre Primary Teachers','Board for Class',
     'Meal Provided and Prepared in School Premises','Establishment', 'School Area', 'School Shifted to New Place',
     'Head Teachers', 'Head Teacher','Is School Residential', 'Residential Type','Total Teachers',
     'Contract Teachers','Management','Village / Town','Cluster','Block','District','State','UDISE Code ',
     'Building','Class Rooms','Boys Toilet', 'Girls Toilet', 'Computer Aided Learning', 'Electricity', 'Wall',
     'Library','Playground','Books in Library','Drinking Water','Ramps for Disable','Computers']
    df = pd.DataFrame(columns=cols)
    return df

all_states = ['dadra-and-nagar-haveli', 'bihar', 'haryana', 'rajasthan', 'mizoram', 'jammu-and-kashmir', 'maharashtra',
 'chandigarh', 'kerala', 'telangana', 'chhattisgarh', 'tamil-nadu', 'arunachal-pradesh', 'uttar-pradesh', 'sikkim',
 'punjab', 'andhra-pradesh', 'delhi', 'assam', 'orissa', 'puducherry', 'manipur', 'andaman-and-nicobar-islands', 'gujrat',
 'nagaland', 'meghalaya', 'jharkhand', 'goa']

def scrape_data(n) :
    try :
        state = all_states[n]
        districts = get_data(state)
        district_wise_schools = []
        
        for district in districts:
            blocks = get_data(district)
            sleep(randint(300, 400))
            for block in blocks:
                clusters = get_data(block)
                print("In block - " + block)
                sleep(randint(60, 120))
                for cluster in clusters:
                    schools = get_data(cluster)
                    district_wise_schools.append(schools)
                    print("In cluster - " + cluster)
                    sleep(randint(30,90))
            print("\nPaused!........................\n")
        
        file = open('pickle_dump/' + state + "_school_list.pkl","wb")
        pickle.dump(district_wise_schools, file)
        file.close()

        df = get_new_df()
        for all_schools in district_wise_schools:
            sleep(randint(120,180))
            for school in all_schools :
                school_data = get_schooldata(school)
                pattern = re.compile('([^:]+)')
                values = []
                values.append(school_data[0])
                for i in school_data[1:] :
                    try :
                        values.append(re.findall(pattern, i)[1])
                    except :
                        values.append(re.findall(pattern, i)[0])

                school_row = pd.Series(values, index=df.columns)
                df = df.append(school_row, ignore_index=True)
                print('Writing school data - ' + school_data[0])
                sleep(10)
        df.to_csv('data/' + state + '_school_data.csv')
    except :
        print("\nAn error occured!\tFAILED!")
        
n = sys.argv[1:]
n = int(n[0])
scrape_data(n)

