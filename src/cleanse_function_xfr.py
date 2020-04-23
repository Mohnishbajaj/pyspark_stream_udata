import datetime
import re

def cleanse_first_last(str):
	return str.capitalize()
def cleanse_country(str):
	return str.capitalize()

def check_cleanse_ip(str):
	str_arr=str.split('.')
	is_ip_valid=1
	if(len(str_arr)==4):
		for i in range(len(str_arr)):
			if(0<=int(str_arr[i])<=255 and is_ip_valid==1):
				is_ip_valid=1
			else:
				is_ip_valid=0
	else:
		is_ip_valid=0
	
	if(is_ip_valid):
		return str
	else:
		return ''


def check_date(str):
	try:
		datetime.datetime.strptime(str, '%d/%m/%Y')
	except ValueError:
		return ''
	return str

def check_cleanse_gender(str):
	mList=['Male','M','m','MALE','male']
	fList=['Female','F','f','FEMALE','female']
	
	if str in mList:
		return 'Male'
	elif str in fList:
		return 'Female'
		
	else:
		return ''

def check_email(str):
	match = re.match('^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$',str )

	if match == None:
		return ''
	else:
		return str
