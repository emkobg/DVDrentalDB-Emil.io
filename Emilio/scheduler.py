import schedule
import time

def create_task():
    print("that was 10 seconds of your life") 


#Time 
schedule.every(10).seconds.do(run.laborer)    

while True:
    schedule.run_pending()
    time.sleep(1)
