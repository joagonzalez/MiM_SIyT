# importing pandas as pd 
import pandas as pd 
  
# Creating the dataframe  
df = pd.read_csv("nba.csv") 
  
# Print the dataframe
print(df) 

# agrupamos por Team

filter_team = df.groupby('Team')
print(filter_team.first())

print(filter_team.get_group('Chicago Bulls'))