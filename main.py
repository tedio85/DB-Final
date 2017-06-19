
# coding: utf-8

# In[50]:

import psycopg2
import time
import threading

class TC():
    def __init__(self):
        self.running = True
        self.check_period = 3
        
    def close(self):
        self.MOI_cursor.close()
        self.MOI_conn.close()
        self.TC_cursor.close()
        self.TC_conn.close()
    
    def connect_to_server(self, host, user, password, dbname, whom='TC'):
        connect_str = "dbname='" + dbname + "' user='" + user + "' host='" + host + "' password='" + password + "'"
        try:
            if whom is 'TC':
                self.TC_conn = psycopg2.connect(connect_str)
                self.TC_cursor = self.TC_conn.cursor()
            else:
                self.MOI_conn = psycopg2.connect(connect_str)
                self.MOI_cursor = self.MOI_conn.cursor()
        except Exception as e:
            print("Uh oh, can't connect. Invalid dbname, user or password?")
    def check_MOI_view(self):
        self.connect_to_server('140.114.77.23', 'db1', 'db001', 'db1', whom='MOI')
        old_rows = None
        while self.running:
            self.MOI_cursor.execute("""SELECT * FROM Accident_Process_Response;""")
            rows = self.MOI_cursor.fetchall()

            # check if status is clear, remove tuple in table(accident_warning, broadcast)
            
            if(old_rows is None):
                hasChange = True
            else:
                hasChange = (rows != old_rows)

            if(hasChange):
                for row in rows:
                    if(row[1] == 'clear'):
                        hasChange = True
                        self.TC_cursor.execute("""UPDATE accident_warning_event
                                                  SET status_of_the_event='clear'
                                                  WHERE accident_id = %s""", str(row[0]))

                        self.TC_cursor.execute("""DELETE FROM warning_broadcast WHERE accident_id = %s""", str(row[0]))
                
                old_rows = rows    
                self.TC_conn.commit();

            time.sleep(self.check_period)
            
    def check_sensor(self):
        self.connect_to_server('140.114.77.23', 'db2', 'db002', 'db2')
        while self.running:
            try:
                query = """select sensor_status.Item_NO, speed_measuring_sensor.Road_ID, 
                speed_measuring_sensor.Road_Direction, speed_measuring_sensor.Milage
                from speed_measuring_sensor, sensor_status
                where speed_measuring_sensor.item_no = sensor_status.item_no
                and speed_measuring_sensor.speed_limit > sensor_status.current_speed;"""

                self.TC_cursor.execute(query)

                rows = self.TC_cursor.fetchall()

                if(len(rows) > 0):
                    self.insert_accident_warning(rows)
            except Exception as e:
                print(e)
            
            time.sleep(self.check_period)
            
    def next_index(self, attri, table):
        query = """select max(""" + attri + """) from """ + table + """;"""
        self.TC_cursor.execute(query)
        
        result = self.TC_cursor.fetchall()[0]
        
        if result[0] is None:
            return str(0)
        
        return str(int(result[0]) + 1)
        
    def insert_accident_warning(self, rows):
        query = """insert into accident_warning_event values (%s, %s, %s, %s, %s, %s);"""
        
        for row in rows:
            self.TC_cursor.execute("""select * from accident_warning_event;""")
            if len(self.TC_cursor.fetchall()) > 0:
                self.TC_cursor.execute("""select * from accident_warning_event where item_no = %s;""", [row[0]])
                if(len(self.TC_cursor.fetchall()) == 0):
                    self.TC_cursor.execute(query, [self.next_index('accident_id','accident_warning_event'), row[0], 'not clear', row[1], row[2], row[3]])
            else:
                self.TC_cursor.execute(query, [self.next_index('accident_id','accident_warning_event'), row[0], 'not clear', row[1], row[2], row[3]])
        
        self.TC_cursor.execute("""select * from accident_warning_event;""")
        rows = self.TC_cursor.fetchall()
        
        self.insert_warning_broadcast(rows)
        
    def insert_warning_broadcast(self, rows):
        query = """insert into warning_broadcast values (%s, %s, %s, %s, %s);"""
        
        for row in rows:
            self.TC_cursor.execute("""SELECT a.road_section_name FROM highway as a, speed_measuring_sensor as b
             WHERE b.item_no = """ + str(row[1]) + """ AND b.location_path = a.location_path_id 
             AND b.milage >= a.from_milage AND b.milage < a.to_milage;""")
            
            road_name = self.TC_cursor.fetchall()[0][0]
            
            self.TC_cursor.execute("""select * from warning_broadcast;""")
            if len(self.TC_cursor.fetchall()) > 0:
                self.TC_cursor.execute("""select * from warning_broadcast where accident_id =%s;""", [row[0]])
                if(len(self.TC_cursor.fetchall()) == 0):
                    self.TC_cursor.execute(query, [self.next_index('broadcast_id','warning_broadcast'), row[0], row[4], road_name, row[2]])
            else:
                self.TC_cursor.execute(query, [self.next_index('broadcast_id','warning_broadcast'), row[0], row[4], road_name, row[2]])
        
        #self.TC_cursor.execute("""select * from warning_broadcast;""")
        #print(self.TC_cursor.fetchall())
        
        self.TC_cursor.execute("""select * from accident_status_information;""")
        self.TC_conn.commit();
        #print((self.TC_cursor.fetchall()))
        
    def get_input(self):
        while self.running:
            k = input("Press Q to quit or enter command\n")
            if k == 'Q' or k == 'q':
                self.running = False
            else:
                self.TC_cursor.execute(k)
                self.TC_conn.commit()
                if(k.startswith('select')):
                    rows = self.TC_cursor.fetchall()
                    print(rows)
                
    def run(self): 
        check_sensor_thread = threading.Thread(target=self.check_sensor, args=())
        check_MOI_thread = threading.Thread(target=self.check_MOI_view, args=())
        input_thread = threading.Thread(target=self.get_input, args=())
        
        check_MOI_thread.start()
        check_sensor_thread.start()
        input_thread.start()
        
        check_MOI_thread.join()
        input_thread.join()
        check_sensor_thread.join()
        self.close()


# In[51]:

model = TC()
model.run()


# In[ ]:



