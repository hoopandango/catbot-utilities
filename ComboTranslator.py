import pandas as pd
import sqlite3
import os
from Crypto.Util.Padding import unpad

schema_combo_main = ['ID','combo_name','combo_effect_ID','combo_size_ID']
schema_combo_units = ['ID','cat_1','cat_2','cat_3','cat_4','cat_5']
schema_combo_effect = ['combo_effect_ID','combo_effect']
schema_combo_size = ['combo_size_ID','combo_size']
#absol = ''
absol = dirname = os.path.dirname(__file__)+"\\"  # use this if relative filepaths don't work on your PC

try:
	conn = sqlite3.connect(absol+'catcombos.db')  
except sqlite3.OperationalError:  # database not found
	print("Database for CatCombos not found.")

def find_and_unpad(file_name):
	# Checks if file is present and unpads it (if needed)
	try:
		fltext = open(absol+file_name).read()
	except:
		return False  # File not found
	
	try:
		fltext = unpad(fltext.encode(),block_size = 16).decode()
	except:
		return True  # File is already unpadded

	fl = open(absol+file_name,'w')
	fl.write(fltext)
	return True  # File unpadded


def update_combo_sizes_table():
	flname = 'Nyancombo2_en.csv'
	if not find_and_unpad(flname):
		return (flname," not found")
	fl = open(absol+flname)
	df = pd.read_csv(fl,header = None,delimiter = '|')
	
	df = df.iloc[:,0:1]
	df.columns = schema_combo_size[1:]

	df.to_sql('combo_sizes', conn, if_exists = 'replace', index = True, index_label = schema_combo_size[0])  # Replace old table with new one
	return('Combo Sizes table updated')


def update_combo_effects_table():
	flname = 'Nyancombo1_en.csv'
	if not find_and_unpad(flname):
		return flname+" not found"
	fl = open(absol+flname)
	df = pd.read_csv(fl,header = None,delimiter = '|')
	
	df = df.iloc[:,0:1]
	df.columns = schema_combo_effect[1:]

	df.to_sql('combo_effects', conn, if_exists = 'replace', index = True, index_label = schema_combo_effect[0])  # Replace old table with new one
	return('Combo Effects table updated')


def update_combo_units_table():
	flname = 'NyancomboData.csv'
	if not find_and_unpad(flname) :
		return (flname,'not found')
	
	fl = open(absol+flname,mode='r')
	fltext = fl.read()
	fltext = fltext.replace(',\n','\n')	 # PONOS put an extra empty field in some rows because
	fl = open(absol+flname,mode='w')
	fl.write(fltext)  # I know this is inefficient but I'm too lazy to think of a faster way to do it and code efficiency isn't important here

	fl = open(absol+flname,mode='r')	
	df = pd.read_csv(fl,header = None,delimiter = ',')

	df = df[df[1] != -1]  # Remove unused combos
	df.drop_duplicates(subset = [2,4,6,8,10,12,13], inplace = True)  # Doesn't check form of unit when dropping duplicate combos as long as they have the same effect
	df.reset_index(inplace = True)
	
	final = pd.DataFrame(columns=schema_combo_units[1:])
	for i in range(5):
		final.iloc[:,i] = 3*df[2*i+2]+df[2*i+3]  # Converts PONOS ID to CatBot ID
	final = final.applymap(lambda x: max(x,-1))  # Converts dummy slots to -1

	final.to_sql('combo_units', conn, if_exists = 'replace', index = True, index_label = schema_combo_units[0])  # Replaces old table with new one
	return('Combo Units table updated')


def update_combo_table():
	flname0 = 'Nyancombo_en.csv'
	flname1 = 'NyancomboData.csv'
	if not (find_and_unpad(flname0) and find_and_unpad(flname1)):
		return (flname0,'or',flname1,'not found')
	
	fl0 = open(absol+flname0) 	# This one has name of combo 
	df0 = pd.read_csv(fl0,header = None,delimiter = '|')
	 
	fl1 = open(absol+flname1)		# This one has units in combo and effect of combo
	df1 = pd.read_csv(fl1,header = None,delimiter = ',')
	
	df1 = df1[df1[1] != -1]  # Removes unused combos
	df0 = df0.iloc[:,0:1]
	df1 = df1.iloc[:,-3:-1]
	df = pd.concat([df0,df1],axis=1,join='inner')  # Merge the two [apparently by using 'inner' it removes the unused combos] 

	df.drop_duplicates(inplace = True)
	df.reset_index(inplace = True,drop = True)
	df.columns = schema_combo_main[1:]

	df.to_sql('combos', conn, if_exists = 'replace', index = True, index_label = schema_combo_main[0])  # Replaces old table with new one
	return('Combos table updated')


print(update_combo_sizes_table())
print(update_combo_effects_table())
print(update_combo_units_table())
print(update_combo_table())
input()  # Stops terminal from closing