import glob
import time
from time import sleep
from kafka import KafkaProducer
import RPi.GPIO as GPIO

sleeptime = 1

GPIO.setmode(GPIO.BCM)
GPIO.setup(4,GPIO.IN, pull_up_down=GPIO.PUD_UP)

print("Waiting for initialization")

producer = KafkaProducer(bootstrap_servers= ['141.40.254.21:9092'])
print("Connected to kafka")

base_dir = '/sys/bus/w1/devices/'
while True:
	try:
		device_folder = glob.glob(base_dir + '28*')[0]
		break
	except IndexError:
		sleep(0.5)
		continue
device_file = device_folder + '/w1_slave'

def TemperatureMeasure():
	f = open(device_file, 'r')
	lines = f.readlines()
	f.close()
	return lines

TemperatureMeasure()

def TemperatureEstimation():
	lines = TemperatureMeasure()
	while lines[0].strip()[-3:]!='YES':
		time.sleep(0.2)
		lines = TemperatureMeasure()
	equals_pos = lines[1].find('t=')
	if equals_pos != -1:
		temp_string = lines[1][equals_pos+2 :]
		temp_c = float(temp_string) / 1000.0
		return temp_c
		
print("Start reading values ...")
try:
	while True:
		print '------------------------'
		est = TemperatureEstimation()
		print ("Temperature",est,"deg. C")
		producer.send('flink-demo', repr(est))
		time.sleep(sleeptime)

except KeyboardInterrupt:
	GPIO.cleanup()