import pandas as pd
import sqlite3

CatBotColumns = ['unit_id','talent_id','max_level','min_first_parameter','max_first_parameter','min_second_parameter','max_second_parameter','min_third_parameter','max_third_parameter','min_fourth_parameter','max_fourth_parameter','description','cost_curve']
l = len(CatBotColumns)

try:
	conn = sqlite3.connect('talents.db')  
except sqlite3.OperationalError:  # database not found
  print("Database for talents not found.")

def updateTalentsTable():
	try:
		df = pd.read_csv('SkillAcquisition.csv')
	except:
		return('SkillAcquisition.csv not found')

	df.dropna(inplace = True)  # Removes padding row and any other problematic rows 

	df = df.astype('int32')  # Turns floats into ints after all the problematic rows are gone

	final = pd.DataFrame(columns = CatBotColumns)

	# Break table into 5 tables and merge them
	for i in range(5):
		s = df.iloc[:,[0]+list(range(l*i+2,l*(i+1)+1))]
		s.columns = CatBotColumns
		final = final.append(s)

	final.sort_values(by = ['unit_id'], inplace = True, kind = 'mergesort')  # Sort table into readable order, needs to be stable

	final.index = list(range(len(final)))  # Reset indices
	
	final.to_sql('talents', conn, if_exists = 'replace', index = True)  # Replace old table with new one
	return('Talents table updated')

def updateLevelsTable():
	try:
		df = pd.read_csv('SkillLevel.csv',index_col=False)
	except:
		return('SkillLevel.csv not found')

	df = df.astype(pd.Int64Dtype())

	df.to_sql('curves', conn, if_exists = 'replace', index = True)

	return('Levelling curves table updated')

print(updateTalentsTable())
print(updateLevelsTable())