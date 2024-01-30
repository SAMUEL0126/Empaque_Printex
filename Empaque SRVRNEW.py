# -*- coding: 850-*-
# -*- coding: utf-8-*-

import socket
import time
import datetime
import pyodbc
import pickle
import Tkinter
import threading
import signal
import os
import signal
import sys

#============================================================================================#

class Comunicaciones():
    def __init__(self):
        self.s=0
        self.Cnxn=0
        self.Cursor=0
        self.IP_Cliente=0
        self.Conex_Cliente=0
        self.Dato_Tx = 0
        self.Dato_Rx = 0
        self.Indicad_Error = 0

#============================================================================================#

    def Open_Socket(self):
        while 1:
            try:
                self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.s.bind(('', 23))
                self.s.listen(30)
                break                
            except:
                pass           
        return
    
    def Open_Cliente(self):
        while 1:
            try:
                self.Conex_Cliente, self.IP_Cliente = self.s.accept()
                list(self.IP_Cliente)
                break                              
            except:
                pass
        return     
    
    def Rx_Dato_Socket(self):
        self.Conex_Cliente.settimeout(3)
        self.Indicad_Error = 0
        try:            
            LongBuffer = 1024
            self.Dato_Rx = self.Conex_Cliente.recv(LongBuffer)
            Texto = str(self.IP_Cliente[0]+" - "+datetime.datetime.now().strftime("%Y-%m-%d")+".txt")
            Log = open(Texto,"a")
            Log.write('RX: ')
            Texto = self.Dato_Rx
            Cadena=str(Texto)
            Log.write (Cadena)
            Log.write(' - ')
            Texto = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            Cadena=str(Texto)
            Log.write (Cadena)
            Log.write('\n')
            Log.close()
            return
        except:
            self.Dato_Rx = "ERROR"
            self.Indicad_Error = "ERROR_CNXN"
            return

    def Tx_Dato_Socket(self):
        self.Indicad_Error = 0
        try:
            self.Conex_Cliente.send(self.Dato_Tx)
            Texto = str(self.IP_Cliente[0]+" - "+datetime.datetime.now().strftime("%Y-%m-%d")+".txt")
            Log = open(Texto,"a")
            Log.write('TX: ')
            Texto = pickle.loads(self.Dato_Tx)
            Cadena=str(Texto)
            Log.write (Cadena)            
            Log.write(' - ')
            Texto = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            Cadena=str(Texto)
            Log.write (Cadena)
            Log.write(' - Tiempo_Respuesta: ')
            CNXNS.Timer_Conex_tout = time.time() - CNXNS.Timer_Conex_tout
            Texto = CNXNS.Timer_Conex_tout
            Cadena=str(Texto)
            Log.write (Cadena)            
            Log.write('\n')
            Log.close()
            return
        except:
            self.Indicad_Error = "ERROR_CNXN"
            print 'Error TX Socket'
            return
    
    def close_socket(self):
        self.Conex_Cliente.close
        return      

    def Open_Conex_Sql(self):
        self.Indicad_Error = 0
        try:
            
            #self.Cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=P1MARPTA0J8A8V\SQLEXPRESS;DATABASE=dbEmpaque_Raspi;UID=sa;PWD=sa')
            self.Cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=10.106.110.16\SQLEXPRESS;DATABASE=dbEmpaque_Raspi;UID=plc;PWD=plc')
            self.Cursor = self.Cnxn.cursor()
            return
        except:
            self.Indicad_Error = "ERROR_CNXN"
            print 'Error conex SQL'            
            return

    def Lectura_IP(self):
        try:
            COM.Cursor.execute("SELECT Codigo_Raspi FROM RaspberryPi WHERE IP_Raspi = (?)", self.IP_Cliente[0])
            CNXNS.Codigo_Mesa = self.Cursor.fetchone()
            if CNXNS.Codigo_Mesa != None:
                CNXNS.Codigo_Mesa = CNXNS.Codigo_Mesa[0]
            else:
                CNXNS.Codigo_Mesa = None
            return
        except:
            self.Indicad_Error = "ERROR_CNXN"
            print 'Error conex IP'            
            return
            

#============================================================================================#

class Hilo_Conexiones(threading.Thread, Comunicaciones):
    def __init__(self):
        threading.Thread.__init__(self)

        self.Row_Verif_Logueo = 0
        self.Row_Verif_Producc = 0
        self.Row_Verif_Paro_Maq = 0
        self.Row_Evento_Produccion = 0
        self.Row_Info_Empl = 0
        self.Row_Info_Lote = 0
        self.Row_Efici_Oper = 0
        self.Trama = 0
        self.Hora_Server = 0
        self.Codigo_Turno = 0
        self.Codigo_Mesa = 0
        self.Est_Maquina = 0
        self.Row_Cant_Event = 0
        self.Cantid_Operario = 0
        self.Turno_Marc_Empl = 0
        self.Estand = "0"
        self.Operac = "0"
        self.Eficiencia = 0
        self.Unidades_Oper = 0
        self.Anterior = 0
        self.Actual = 0
        self.Fecha_Inic = 0
        self.Fecha_Final = 0
        self.Tiemp_Paro_Alim = 0
        self.Verif_Paro_Aliment = 0
        self.Timer_Conex_tout = 0
        self.Indic_Error_Efic = 0
        self.Cantid_Acumulada=0
        self.Dia_Anterior = 0
        self.Dia_Actual = 0
        self.Fecha_Query = 0
        self.Row_Verif_Prod_Inic = 0
        self.Row_MSTR_Paros = 0
        self.Tiempo_Tot_Turno = 0
        self.Tiempo_Alimen_Dia = 0
        self.Minutos_Presencia = 0
        self.Unidades_x_Stdr = 0
        self.Reg_Paros_Afect_efic = 0
        self.Contador_Paros_Alim = 0
        self.Unidades_Turno = 0
        self.Eficiencia_Turno = 0
        self.Tiempo_Par_Afect_efic = 0
        self.Tiemp_Tot_Par_Sin_Efic = 0
        self.Copy_Turno = 0
        self.Registr_Min_Pres = 0
        
#============================================================================================#

    def run(self):
        while 1:
            COM.Open_Cliente()
            if CLSTURNO.Indicad_Cnxn != 0:
                COM.Dato_Tx = pickle.dumps("ERROR_CNXN*")
                COM.Tx_Dato_Socket()
                COM.Conex_Cliente.close
            else:
                self.Timer_Conex_tout = time.time()                
                CLSTURNO.Indicad_Cnxn = 2
                COM.Open_Conex_Sql()
                COM.Lectura_IP()
                COM.Cnxn.close
                if COM.Indicad_Error == "ERROR_CNXN":
                    COM.Dato_Tx = pickle.dumps("ERROR_CNXN*")
                    COM.Tx_Dato_Socket()
                    COM.Conex_Cliente.close
                    COM.Cnxn.close
                else:
                    if self.Codigo_Mesa != None:
                        COM.Rx_Dato_Socket()
                        if 'EG' in COM.Dato_Rx:
                            CNXNS.Sol_Est_Gral()                        
                            self.Timer_Conex_tout = time.time() - self.Timer_Conex_tout   
                            CLSTURNO.Indicad_Cnxn = 0
                        elif 'STB' in COM.Dato_Rx:
                            CNXNS.Est_Standby()
                            self.Timer_Conex_tout = time.time() - self.Timer_Conex_tout                            
                            CLSTURNO.Indicad_Cnxn = 0
                        elif 'LOG' in COM.Dato_Rx:
                            CNXNS.Est_Logueado()
                            self.Timer_Conex_tout = time.time() - self.Timer_Conex_tout
                            CLSTURNO.Indicad_Cnxn = 0                                                   
                        elif "ADMLGT" in COM.Dato_Rx:
                            CNXNS.Deslog_Usuario()
                            self.Timer_Conex_tout = time.time() - self.Timer_Conex_tout
                            CLSTURNO.Indicad_Cnxn = 0
                        else:
                            COM.Conex_Cliente.close
                            CLSTURNO.Indicad_Cnxn = 0
                    else:
                        COM.Conex_Cliente.close
                        CLSTURNO.Indicad_Cnxn = 0

#============================================================================================#
                    
    def Calcula_Eficienc(self):
        self.Indic_Error_Efic = 0
        try:
            self.Anterior = datetime.datetime.now()- datetime.timedelta(days=1)
            self.Actual = datetime.datetime.now()
            if self.Anterior.month <= 9 and self.Anterior.day <= 9:
                self.Fecha_Inic = ("0"+str(self.Anterior.day)+"/0"+str(self.Anterior.month)+"/"+str(self.Anterior.year)+" "+"22:00:00")
            elif self.Anterior.month <= 9:
                self.Fecha_Inic = (str(self.Anterior.day)+"/0"+str(self.Anterior.month)+"/"+str(self.Anterior.year)+" "+"22:00:00")
            elif self.Anterior.day <= 9:
                self.Fecha_Inic = ("0"+str(self.Anterior.day)+"/"+str(self.Anterior.month)+"/"+str(self.Anterior.year)+" "+"22:00:00")
            else:
                self.Fecha_Inic = (str(self.Anterior.day)+"/"+str(self.Anterior.month)+"/"+str(self.Anterior.year)+" "+"22:00:00")
                
            if self.Actual.month <= 9 and self.Actual.day <= 9:
                self.Fecha_Final = ("0"+str(self.Actual.day)+"/0"+str(self.Actual.month)+"/"+str(self.Actual.year)+" "+"22:00:59")
            elif self.Actual.month <= 9:
                self.Fecha_Final = (str(self.Actual.day)+"/0"+str(self.Actual.month)+"/"+str(self.Actual.year)+" "+"22:00:59")
            elif self.Actual.day <= 9:
                self.Fecha_Final = ("0"+str(self.Actual.day)+"/"+str(self.Actual.month)+"/"+str(self.Actual.year)+" "+"22:00:59")
            else:
                self.Fecha_Final = (str(self.Actual.day)+"/"+str(self.Actual.month)+"/"+str(self.Actual.year)+" "+"22:00:59")

            self.Tiempo_Tot_Turno = 0
            self.Tiempo_Alimen_Dia = 0
            self.Minutos_Presencia = 0
            self.Unidades_x_Stdr = 0
            self.Unidades_Turno = 0
            self.Eficiencia_Turno = 0
            self.Tiempo_Par_Afect_efic = 0
            self.Tiemp_Tot_Par_Sin_Efic = 0
            self.Registr_Min_Pres = 0
            COM.Cursor.execute("WITH C AS(SELECT Event_Prod.Id_Lote, Event_Prod.Codigo_Turno AS Turno, Marc.Fecha_Ingreso AS Marc_Ingreso, Marc.Fecha_Salida AS Marc_Salida, DATEDIFF(ss,Marc.Fecha_Ingreso, ISNULL(Marc.Fecha_Salida, GETDATE()))/60 AS 'Tiempo_Logueo', left(T_Empl.Hora_Inicio,8) AS Ini_Turno, left(T_Empl.Hora_Final,8) AS Fin_Turno, T_Empl.Tiempo_Turno_Minutos AS Minutos_Turno, CASE WHEN (Marc.Fecha_Salida IS NOT NULL) AND (T_Empl.Hora_Inicio != '00:00:00') AND ((DATEDIFF(ss,T_Empl.Hora_Inicio,CONVERT(varchar(25), GETDATE(), 108)))/60) >= T_Empl.Tiempo_Turno_Minutos THEN T_Empl.Tiempo_Turno_Minutos WHEN (Marc.Fecha_Salida IS NOT NULL ) AND (T_Empl.Hora_Inicio != '00:00:00') AND ((DATEDIFF(ss,T_Empl.Hora_Inicio,CONVERT(varchar(25), GETDATE(), 108)))/60) < T_Empl.Tiempo_Turno_Minutos THEN  ((DATEDIFF(ss,T_Empl.Hora_Inicio,CONVERT(varchar(25), GETDATE(), 108)))/60) WHEN (Marc.Fecha_Salida IS NOT NULL ) AND (T_Empl.Hora_Inicio = '00:00:00') AND ((DATEDIFF(ss,CONVERT(varchar(25), Marc.Fecha_Ingreso, 108),CONVERT(varchar(25), GETDATE(), 108)))/60) >= T_Empl.Tiempo_Turno_Minutos THEN  T_Empl.Tiempo_Turno_Minutos WHEN (Marc.Fecha_Salida IS NOT NULL ) AND (T_Empl.Hora_Inicio = '00:00:00') AND ((DATEDIFF(ss,CONVERT(varchar(25), Marc.Fecha_Ingreso, 108),CONVERT(varchar(25), GETDATE(), 108)))/60) < T_Empl.Tiempo_Turno_Minutos THEN ((DATEDIFF(ss,CONVERT(varchar(25), Marc.Fecha_Ingreso, 108),CONVERT(varchar(25), GETDATE(), 108)))/60) WHEN (T_Empl.Hora_Inicio != '00:00:00') AND((DATEDIFF(ss,T_Empl.Hora_Inicio,CONVERT(varchar(25), GETDATE(), 108)))/60) >= T_Empl.Tiempo_Turno_Minutos THEN T_Empl.Tiempo_Turno_Minutos WHEN (T_Empl.Hora_Inicio != '00:00:00') AND((DATEDIFF(ss,T_Empl.Hora_Inicio,CONVERT(varchar(25), GETDATE(), 108)))/60) < T_Empl.Tiempo_Turno_Minutos THEN ((DATEDIFF(ss,T_Empl.Hora_Inicio,CONVERT(varchar(25), GETDATE(), 108)))/60) WHEN (T_Empl.Hora_Inicio = '00:00:00') AND((DATEDIFF(ss,CONVERT(varchar(25), Marc.Fecha_Ingreso, 108),CONVERT(varchar(25), GETDATE(), 108)))/60) >= T_Empl.Tiempo_Turno_Minutos THEN T_Empl.Tiempo_Turno_Minutos ELSE ((DATEDIFF(ss,CONVERT(varchar(25), Marc.Fecha_Ingreso, 108),CONVERT(varchar(25), GETDATE(), 108)))/60) END AS Tiempo_Turno, 0 AS T_Alimentacion, Event_Prod.Identificacion, Event_Prod.Estandar, Lote.Unidades AS Unidades_Plega, (COUNT(Event_Prod.Id_Lote)) AS 'Contador', Lote.Unidades*(COUNT(Event_Prod.Id_Lote)) AS 'Tot_Unidades', (Event_Prod.Estandar*(Lote.Unidades*(COUNT(Event_Prod.Id_Lote)))) AS 'Minut_Produc' FROM Eventos_Produccion AS Event_Prod INNER JOIN Lote AS Lote ON Event_Prod.Id_Lote = Lote.Id_Lote INNER JOIN Turno_Empleado AS T_Empl ON Event_Prod.Codigo_Turno = T_Empl.Codigo_Turno INNER JOIN Marcaciones AS Marc ON (Event_Prod.Identificacion = Marc.Identificacion AND Event_Prod.Codigo_Turno = Marc.Codigo_Turno) WHERE Event_Prod.Ini_Event_Prod >= (?) AND Event_Prod.Ini_Event_Prod <= (?) AND Marc.Fecha_Ingreso >= (?) AND Marc.Fecha_Ingreso <= (?) AND Event_Prod.Identificacion = (?) AND Event_Prod.Fin_Event_Prod IS NOT NULL GROUP BY Event_Prod.Id_Lote, Event_Prod.Codigo_Turno, Marc.Fecha_Ingreso, Marc.Fecha_Salida, T_Empl.Hora_Inicio, T_Empl.Hora_Final, T_Empl.Tiempo_Turno_Minutos, T_Empl.Tiempo_Alimentacion, Event_Prod.Identificacion, Event_Prod.Estandar, Lote.Unidades) SELECT Identificacion, Turno, Ini_Turno, Fin_Turno, Minutos_Turno AS 'Tiempo_Turno (min)', (T_Alimentacion) AS 'Tiempo_Alim', (Marc_Ingreso) AS 'Fecha_Marc_Ingreso', (Marc_Salida) AS 'Fecha_Marc_Salida', (Tiempo_Logueo) AS 'Min_Presencia_Login', (Tiempo_Turno) AS 'Min_Presencia_Turno', SUM(CONVERT(Float,(Minut_Produc))) AS 'Unidades_x_std', SUM(Tot_Unidades) AS 'Unidades_x_Turno' FROM C GROUP BY Identificacion, Turno, Ini_Turno, Fin_Turno, Minutos_Turno, (T_Alimentacion), (Marc_Ingreso), (Marc_Salida), (Tiempo_Logueo), (Tiempo_Turno) ORDER BY Turno", self.Fecha_Inic, self.Fecha_Final, self.Fecha_Inic, self.Fecha_Final, self.Row_Verif_Logueo[1])
            self.Row_Event_P_Dia = COM.Cursor.fetchall()
            COM.Cursor.execute("SELECT Codigo_Mstr_Paro, Codigo_Turno, SUM(DATEDIFF(SS,Ini_Paro_Maq, ISNULL(Fin_Paro_Maq, GETDATE()))/60) AS Tiempo_Paro, COUNT (Codigo_Turno) AS Repeticiones FROM Paros_Maquina WHERE Identificacion = (?) AND Codigo_Mstr_Paro = (?) AND Ini_Paro_Maq >= (?) AND Ini_Paro_Maq <= (?) GROUP BY Codigo_Mstr_Paro, Codigo_Turno", self.Row_Verif_Logueo[1], '102', self.Fecha_Inic, self.Fecha_Final)
            self.Row_Par_Alim_Dia = COM.Cursor.fetchall()                         
            if self.Row_Event_P_Dia != None:
                self.Copy_Turno = 0                
                for self.Reg_Event_P_Dia in self.Row_Event_P_Dia:
                    if self.Reg_Event_P_Dia[1]!= self.Copy_Turno:
                        self.Unidades_x_Stdr = self.Unidades_x_Stdr + float(self.Reg_Event_P_Dia[10])                        
                        self.Unidades_Turno = self.Unidades_Turno + int(self.Reg_Event_P_Dia[11])                        
                        self.Tiempo_Tot_Turno = self.Tiempo_Tot_Turno + int(self.Reg_Event_P_Dia[4])
                        self.Minutos_Presencia = self.Minutos_Presencia + int(self.Reg_Event_P_Dia[9])
                        self.Registr_Min_Pres = self.Reg_Event_P_Dia[9]
                        self.Copy_Turno = self.Reg_Event_P_Dia[1]
                    else:
                        if int(self.Reg_Event_P_Dia[9]) > int(self.Registr_Min_Pres):
                            self.Minutos_Presencia = self.Minutos_Presencia - int(self.Registr_Min_Pres)
                            self.Minutos_Presencia = self.Minutos_Presencia + int(self.Reg_Event_P_Dia[9])
                            self.Registr_Min_Pres = int(self.Reg_Event_P_Dia[9])
                        else:
                            pass
                    
                if self.Row_Par_Alim_Dia != None:
                    self.Contador_Paros_Alim = 0
                    for self.Reg_Tiemp_Alim in self.Row_Par_Alim_Dia:
                        self.Tiempo_Alimen_Dia = self.Tiempo_Alimen_Dia + self.Reg_Tiemp_Alim[2]
                        self.Contador_Paros_Alim = self.Contador_Paros_Alim + 1
                else:
                    pass                    
            else:
                pass
            print ''
            print "Unid_X_STDR: ", self.Unidades_x_Stdr
            print "Tiempo_Turno: ", self.Tiempo_Tot_Turno
            print "Minut_Pesencia: ", self.Minutos_Presencia
            
            if self.Tiempo_Alimen_Dia != 0:
                if self.Tiempo_Tot_Turno >= 480 and self.Tiempo_Tot_Turno < 720:
                    if (self.Tiempo_Alimen_Dia >= 30) or (self.Tiempo_Alimen_Dia < 30 and self.Contador_Paros_Alim > 1):
                        self.Minutos_Presencia = self.Minutos_Presencia - 30                        
                    else:
                        self.Minutos_Presencia = self.Minutos_Presencia - self.Tiempo_Alimen_Dia
                elif self.Tiempo_Tot_Turno >= 720:
                    if (self.Tiempo_Alimen_Dia >= 60) or (self.Tiempo_Alimen_Dia < 60 and self.Contador_Paros_Alim > 2):
                        self.Minutos_Presencia = self.Minutos_Presencia - 60
                    else:
                        self.Minutos_Presencia = self.Minutos_Presencia - self.Tiempo_Alimen_Dia
                else:
                    pass
            else:
                pass
            COM.Cursor.execute("SELECT Codigo_Mstr_Paro, Descripcion, Color_Alarma, Afecta_Eficiencia FROM Mstr_Paros WHERE Afecta_Eficiencia = 'True'")
            self.Row_MSTR_Paros = COM.Cursor.fetchall()
            if self.Row_MSTR_Paros != None:
                for self.Reg_Paros_Afect_efic in self.Row_MSTR_Paros:                    
                    if self.Reg_Paros_Afect_efic[3] == True:
                        COM.Cursor.execute("SELECT Codigo_Mstr_Paro, SUM(DATEDIFF(SS,Ini_Paro_Maq, ISNULL(Fin_Paro_Maq, GETDATE()))/60) AS Tiempo_Paro, COUNT (Codigo_Turno) AS Repeticiones FROM Paros_Maquina WHERE Identificacion = (?) AND Codigo_Mstr_Paro = (?) AND Ini_Paro_Maq >= (?) AND Ini_Paro_Maq <= (?) GROUP BY Codigo_Mstr_Paro", self.Row_Verif_Logueo[1], self.Reg_Paros_Afect_efic[0], self.Fecha_Inic, self.Fecha_Final)
                        self.Tiempo_Par_Afect_efic = COM.Cursor.fetchone()
                        if self.Tiempo_Par_Afect_efic != None:
                            self.Tiemp_Tot_Par_Sin_Efic = self.Tiemp_Tot_Par_Sin_Efic + self.Tiempo_Par_Afect_efic[1]                            
                        else:
                            pass
                    else:
                        pass
            else:
                pass
            self.Minutos_Presencia = self.Minutos_Presencia - self.Tiemp_Tot_Par_Sin_Efic
            print "T_Alimentac: ", self.Tiempo_Alimen_Dia
            print "T_Paros_Afec_Efi: ",self.Tiemp_Tot_Par_Sin_Efic
            print "Minut_Pesenc_Discriminado: ", self.Minutos_Presencia
            if self.Minutos_Presencia != 0:
                self.Eficiencia_Turno = round(float((self.Unidades_x_Stdr/self.Minutos_Presencia)*100),0)
                print "Eficienc_Oper: ", self.Row_Efici_Oper
                print 'CC:',self.Row_Verif_Logueo[1],"- Unid_x_ST:",self.Unidades_x_Stdr,"- Min_Presenc:",self.Minutos_Presencia,"- Unid_Turno:",self.Unidades_Turno,"- Efic:",self.Eficiencia_Turno,"- T_Alim:",self.Tiempo_Alimen_Dia
                self.Row_Efici_Oper = [self.Row_Verif_Logueo[1], self.Unidades_x_Stdr, self.Minutos_Presencia, self.Unidades_Turno, self.Eficiencia_Turno]                
            else:
                print "Eficienc_Oper: ", self.Row_Efici_Oper
                self.Row_Efici_Oper = [self.Row_Verif_Logueo[1], self.Unidades_x_Stdr, self.Minutos_Presencia, self.Unidades_Turno, self.Eficiencia_Turno] 
            if self.Row_Efici_Oper != 0:                
                COM.Cursor.execute("SELECT top(1) MAX(Id_Event_Prod) FROM Eventos_Produccion WHERE Ini_Event_Prod >= (?) AND Ini_Event_Prod <= (?) AND Identificacion = (?) AND Fin_Event_Prod IS NOT NULL", self.Fecha_Inic, self.Fecha_Final, self.Row_Verif_Logueo[1])
                self.Row_Update_Efic = COM.Cursor.fetchone()
                self.Unidades_x_Stdr = round(self.Unidades_x_Stdr)

                if self.Row_Update_Efic != None:
                    #COM.Cursor.execute("UPDATE Eventos_Produccion set Eficiencia = (?) WHERE Id_Event_Prod = (?)", self.Row_Efici_Oper[4], self.Row_Update_Efic[0])
                    COM.Cursor.execute("UPDATE Eventos_Produccion set Eficiencia = (?), min_prod = (?), Min_Presencia = (?) WHERE Id_Event_Prod = (?)", self.Row_Efici_Oper[4],self.Unidades_x_Stdr,self.Minutos_Presencia,self.Row_Update_Efic[0])
                    
                    COM.Cnxn.commit()
                else:
                    pass
            else:
                pass
            return
            
        except:
            self.Indic_Error_Efic = "ERROR_EFIC"            
            print 'Error C lculo Efic'
            return
        
#============================================================================================#

    def Sol_Est_Gral(self):
        COM.Open_Conex_Sql()
        if COM.Indicad_Error == "ERROR_CNXN":
            COM.Dato_Tx = pickle.dumps("ERROR_CNXN*")
            COM.Tx_Dato_Socket()
            COM.Conex_Cliente.close
            COM.Cnxn.close
        else:
            COM.Cursor.execute("SELECT TOP(1) Id_Paro_Maq, Codigo_Mstr_Paro, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Fin_Paro_Maq FROM Paros_Maquina WHERE Codigo_Raspi = (?) ORDER BY Id_Paro_Maq DESC", self.Codigo_Mesa)
            self.Row_Paro_Maq = COM.Cursor.fetchone()        
            if self.Row_Paro_Maq != None and self.Row_Paro_Maq.Fin_Paro_Maq == None:
                COM.Cursor.execute("SELECT Codigo_Mstr_Paro, Descripcion from Mstr_Paros WHERE Codigo_Mstr_Paro = (?)", self.Row_Paro_Maq.Codigo_Mstr_Paro)
                self.Row_Mstr_Paro_Maq = COM.Cursor.fetchone()
                self.Est_Maquina = self.Row_Mstr_Paro_Maq.Descripcion
                if self.Est_Maquina == "Generico":
                    self.Est_Maquina = "Paro Generico"
                else:
                    pass
            else:
                self.Est_Maquina = "Maq. Activa"          
            COM.Cursor.execute("SELECT TOP(1) Id_Marcacion, Identificacion, Codigo_Turno, Codigo_Raspi, Fecha_Ingreso, Fecha_Salida, Marc_Modificada FROM Marcaciones WHERE Codigo_Raspi = (?) ORDER BY Id_Marcacion DESC", self.Codigo_Mesa)            
            self.Row_Verif_Logueo = COM.Cursor.fetchone()
            COM.Cursor.execute("SELECT TOP(1) Id_Event_Prod, Id_Lote, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Event_Prod, Fin_Event_Prod, Cantidad_Rasp, Operacion, Estandar, Tipo_Lect FROM Eventos_Produccion WHERE Codigo_Raspi = (?) ORDER BY Id_Event_Prod DESC", self.Codigo_Mesa)            
            self.Row_Verif_Producc = COM.Cursor.fetchone()       
            if self.Row_Verif_Logueo != None and self.Row_Verif_Logueo.Fecha_Salida == None:
                CNXNS.Calcula_Eficienc()
                COM.Cnxn.close
                if CNXNS.Indic_Error_Efic == "ERROR_EFIC":
                    COM.Dato_Tx = pickle.dumps("ERROR_CNXN*")
                    COM.Tx_Dato_Socket()
                    COM.Conex_Cliente.close
                else:         
                    if self.Row_Verif_Producc != None and self.Row_Verif_Producc.Fin_Event_Prod == None:                
                        COM.Cursor.execute("SELECT Nombres, Primer_Apellido FROM Empleados  WHERE Identificacion = (?)", self.Row_Verif_Logueo[1])
                        self.Row_Info_Empl = COM.Cursor.fetchone()
                        COM.Cursor.execute("SELECT Codigo_Lote, Categ_Lote, Referenc_Lote, Talla_Lote, Color_Lote, Cantidad_Lote, Unidades, EAN FROM Lote  WHERE Id_Lote = (?)", self.Row_Verif_Producc[1])
                        self.Row_Info_Lote = COM.Cursor.fetchone()                
                        COM.Cnxn.close
                        if self.Row_Verif_Producc[7] != 0:
                                self.Row_Verif_Producc[7] = self.Row_Verif_Producc[7] - 1
                        else:
                           pass
                        if self.Row_Efici_Oper != None:
                            if self.Row_Verif_Producc[10] != "EAN":
                                COM.Dato_Tx = pickle.dumps("LOGPRODI*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Lote[0])+"*"+str(self.Row_Info_Lote[2])+"*"+str(self.Row_Info_Lote[5])+"*"+str(self.Est_Maquina+"*")+str(self.Row_Verif_Producc[7])+"*"+str(self.Row_Verif_Logueo[2])+"*"+str(CNXNS.Codigo_Mesa)+"*"+str(self.Row_Verif_Producc[8])+"*"+str(self.Row_Verif_Producc.Estandar)+"*"+str(self.Row_Efici_Oper[3])+"*"+str(self.Row_Efici_Oper[4])+"*")
                                COM.Tx_Dato_Socket ()
                                COM.Conex_Cliente.close
                                return
                            else:
                                COM.Dato_Tx = pickle.dumps("LOGPRODI*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Lote[6])+"*"+str(self.Row_Info_Lote[2])+"*"+str(self.Row_Info_Lote[5])+"*"+str(self.Est_Maquina+"*")+str(self.Row_Verif_Producc[7])+"*"+str(self.Row_Verif_Logueo[2])+"*"+str(CNXNS.Codigo_Mesa)+"*"+str(self.Row_Verif_Producc[8])+"*"+str(self.Row_Verif_Producc.Estandar)+"*"+str(self.Row_Efici_Oper[3])+"*"+str(self.Row_Efici_Oper[4])+"*")
                                COM.Tx_Dato_Socket ()
                                COM.Conex_Cliente.close
                                return
                            
                        else:
                            if self.Row_Verif_Producc[10] != "EAN":                    
                                COM.Dato_Tx = pickle.dumps("LOGPRODI*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Lote[0])+"*"+str(self.Row_Info_Lote[2])+"*"+str(self.Row_Info_Lote[5])+"*"+str(self.Est_Maquina+"*")+str(self.Row_Verif_Producc[7])+"*"+str(self.Row_Verif_Logueo[2])+"*"+str(CNXNS.Codigo_Mesa)+"*"+str(self.Row_Verif_Producc[8])+"*"+str(self.Row_Verif_Producc.Estandar)+"*"+"0"+"*"+"0"+"*")
                                COM.Tx_Dato_Socket ()
                                COM.Conex_Cliente.close
                                return
                            else:
                                COM.Dato_Tx = pickle.dumps("LOGPRODI*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Lote[6])+"*"+str(self.Row_Info_Lote[2])+"*"+str(self.Row_Info_Lote[5])+"*"+str(self.Est_Maquina+"*")+str(self.Row_Verif_Producc[7])+"*"+str(self.Row_Verif_Logueo[2])+"*"+str(CNXNS.Codigo_Mesa)+"*"+str(self.Row_Verif_Producc[8])+"*"+str(self.Row_Verif_Producc.Estandar)+"*"+"0"+"*"+"0"+"*")
                                COM.Tx_Dato_Socket ()
                                COM.Conex_Cliente.close
                                return                        
                    else:
                        COM.Cursor.execute("SELECT Nombres, Primer_Apellido FROM Empleados  WHERE Identificacion = (?)", self.Row_Verif_Logueo[1])
                        self.Row_Info_Empl = COM.Cursor.fetchone()
                        COM.Cnxn.close
                        if self.Row_Efici_Oper != None:
                            COM.Dato_Tx = pickle.dumps("LOG*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+"Ninguno"+"*"+"Ninguno"+"*"+"0"+"*"+str(self.Est_Maquina)+"*"+"0"+"*"+str(self.Row_Verif_Logueo[2])+"*"+str(CNXNS.Codigo_Mesa)+"*"+"N/A"+"*"+"0"+"*"+str(self.Row_Efici_Oper[3])+"*"+str(self.Row_Efici_Oper[4])+"*")
                            COM.Tx_Dato_Socket()
                            COM.Conex_Cliente.close
                            return
                        else:
                            COM.Dato_Tx = pickle.dumps("LOG*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+"Ninguno"+"*"+"Ninguno"+"*"+"0"+"*"+str(self.Est_Maquina)+"*"+"0"+"*"+str(self.Row_Verif_Logueo[2])+"*"+str(CNXNS.Codigo_Mesa)+"*"+"N/A"+"*"+"0"+"*"+"0"+"*"+"0"+"*")
                            COM.Tx_Dato_Socket()
                            COM.Conex_Cliente.close
                            return
            else:
                if self.Row_Verif_Producc != None and self.Row_Verif_Producc.Fin_Event_Prod == None:
                    COM.Cursor.execute("SELECT Codigo_Lote, Categ_Lote, Referenc_Lote, Talla_Lote, Color_Lote, Cantidad_Lote, Unidades, EAN FROM Lote  WHERE Id_Lote = (?)", self.Row_Verif_Producc[1])
                    self.Row_Info_Lote = COM.Cursor.fetchone()
                    COM.Cnxn.close
                    if self.Row_Verif_Producc[10] != "EAN":
                        COM.Dato_Tx = pickle.dumps("STBPRODI*"+"Generico"+"*"+" "+"*"+str(self.Row_Info_Lote[0])+"*"+str(self.Row_Info_Lote[2])+"*"+str(self.Row_Info_Lote[5])+"*"+str(self.Est_Maquina)+"*"+str(self.Row_Verif_Producc[7])+"*"+"N/A"+"*"+str(CNXNS.Codigo_Mesa)+"*"+str(self.Row_Verif_Producc[8])+"*"+"0"+"*"+"0"+"*"+"0"+"*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                        return
                    else:
                        COM.Dato_Tx = pickle.dumps("STBPRODI*"+"Generico"+"*"+" "+"*"+str(self.Row_Info_Lote[6])+"*"+str(self.Row_Info_Lote[2])+"*"+str(self.Row_Info_Lote[5])+"*"+str(self.Est_Maquina)+"*"+str(self.Row_Verif_Producc[7])+"*"+"N/A"+"*"+str(CNXNS.Codigo_Mesa)+"*"+str(self.Row_Verif_Producc[8])+"*"+"0"+"*"+"0"+"*"+"0"+"*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                        return
                else:
                    COM.Cnxn.close
                    COM.Dato_Tx = pickle.dumps("STB*"+"Generico"+"*"+" "+"*"+"Ninguno"+"*"+"Ninguno"+"*"+"0"+"*"+str(self.Est_Maquina)+"*"+"0"+"*"+"N/A"+"*"+str(CNXNS.Codigo_Mesa)+"*"+"N/A"+"*"+"0"+"*"+"0"+"*"+"0"+"*")
                    COM.Tx_Dato_Socket()
                    COM.Conex_Cliente.close
                    return

#============================================================================================#

    def Est_Standby(self):
        self.Trama = COM.Dato_Rx.split("*")
        if self.Trama[1] == "12300":
            COM.Dato_Tx = pickle.dumps("ADM*")
            COM.Tx_Dato_Socket()
            COM.Cnxn.close
            COM.Conex_Cliente.close
            return
        elif self.Trama[1] == "78900":
            COM.Dato_Tx = pickle.dumps("ADM*")
            COM.Tx_Dato_Socket()
            COM.Cnxn.close
            COM.Conex_Cliente.close
            return
        else:
            pass
        COM.Open_Conex_Sql()
        if COM.Indicad_Error == "ERROR_CNXN":
            COM.Dato_Tx = pickle.dumps("ERROR_CNXN*")
            COM.Tx_Dato_Socket()
            COM.Conex_Cliente.close
            COM.Cnxn.close
        else:
            COM.Cursor.execute("SELECT Identificacion, Nombres, Primer_Apellido, Segundo_Apellido, Centro_Costos, Administrador from Empleados WHERE Identificacion = (?)", self.Trama[1])            
            self.Row_Info_Empl = COM.Cursor.fetchone()                
            if self.Row_Info_Empl != None:
                if self.Row_Info_Empl.Administrador == True:        
                    COM.Dato_Tx = pickle.dumps("ADM*")
                    COM.Tx_Dato_Socket()
                    COM.Cnxn.close
                    COM.Conex_Cliente.close
                    return            
                else:
                    COM.Cursor.execute("SELECT TOP(1) Id_Marcacion, Identificacion, Codigo_Turno, Codigo_Raspi, Fecha_Ingreso, Fecha_Salida FROM Marcaciones WHERE Identificacion = (?) ORDER BY Id_Marcacion DESC", self.Row_Info_Empl[0])
                    self.Row_Marcacion = COM.Cursor.fetchone()
                    COM.Cursor.execute("SELECT TOP(1) Id_Event_Prod, Id_Lote, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Event_Prod, Fin_Event_Prod, Cantidad_Rasp, Operacion, Estandar, Tipo_Lect FROM Eventos_Produccion WHERE Codigo_Raspi = (?) ORDER BY Id_Event_Prod DESC", self.Codigo_Mesa)            
                    self.Row_Verif_Producc = COM.Cursor.fetchone()
                    COM.Cursor.execute("SELECT TOP(1) Id_Paro_Maq, Codigo_Mstr_Paro, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Fin_Paro_Maq FROM Paros_Maquina WHERE Codigo_Raspi = (?) ORDER BY Id_Paro_Maq DESC", self.Codigo_Mesa)
                    self.Row_Paro_Maq = COM.Cursor.fetchone()
                    if self.Row_Paro_Maq != None:
                        COM.Cursor.execute("SELECT Descripcion from Mstr_Paros WHERE Codigo_Mstr_Paro = (?)", self.Row_Paro_Maq[1])
                        self.Row_Mstr_Paro_Maq = COM.Cursor.fetchone()
                        if self.Row_Mstr_Paro_Maq[0] == "Generico":
                            self.Row_Mstr_Paro_Maq[0] = "Paro Generico"
                        else:
                            pass
                    else:
                        pass                
                    if self.Row_Marcacion != None and self.Row_Marcacion.Fecha_Salida == None and self.Row_Marcacion.Codigo_Raspi != self.Codigo_Mesa:
                        COM.Cnxn.close
                        COM.Dato_Tx = pickle.dumps("USRLOG*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                        return                       
                    else:
                        if self.Trama[2] == "0":
                            COM.Dato_Tx = pickle.dumps("LT*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Empl[2])+"*")                            
                            COM.Tx_Dato_Socket()   
                            COM.Cnxn.close
                            COM.Conex_Cliente.close
                            return
                        else:
                            COM.Cursor.execute("SELECT TOP(1) Codigo_Turno, Descripcion_Turno, Hora_Inicio, Hora_Final FROM Turno_Empleado WHERE Codigo_Turno = (?)", self.Trama[2])
                            self.Turno_Marc_Empl = COM.Cursor.fetchone()
                            if self.Turno_Marc_Empl != None:                
                                if self.Row_Verif_Producc != None and self.Row_Verif_Producc.Fin_Event_Prod == None:
                                    if self.Row_Paro_Maq != None and self.Row_Paro_Maq.Fin_Paro_Maq == None:                            
                                        COM.Cursor.execute("UPDATE Paros_Maquina set Fin_Paro_Maq = (?) WHERE Id_Paro_Maq = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Paro_Maq[0])           
                                        COM.Cnxn.commit()                   
                                        COM.Cursor.execute("UPDATE Eventos_Produccion set Fin_Event_Prod = (?) WHERE Id_Event_Prod = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Verif_Producc[0])             
                                        COM.Cnxn.commit()
                                        COM.Cursor.execute("INSERT INTO Marcaciones(Identificacion, Codigo_Turno, Codigo_Raspi, Fecha_Ingreso) VALUES(?,?,?,?)", self.Row_Info_Empl[0], self.Trama[2], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"))           
                                        COM.Cnxn.commit()
                                        COM.Cursor.execute("INSERT INTO Eventos_Produccion(Id_Lote, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Event_Prod, Cantidad_Rasp, Operacion, Estandar, Tipo_Lect) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", self.Row_Verif_Producc[1], self.Row_Info_Empl[0], self.Trama[2], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Verif_Producc[7], self.Row_Verif_Producc[8], self.Row_Verif_Producc[9], self.Row_Verif_Producc[10])           
                                        COM.Cnxn.commit()
                                        COM.Cursor.execute("INSERT INTO Paros_Maquina(Codigo_Mstr_Paro, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Id_Lote) VALUES (?, ?, ?, ?, ?, ?)", self.Row_Paro_Maq[1], self.Row_Info_Empl[0], self.Trama[2], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Verif_Producc[1])
                                        COM.Cnxn.commit()
                                        COM.Cnxn.close
                                        COM.Dato_Tx = pickle.dumps("LI*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Empl[2])+"*"+str(self.Row_Info_Empl[3])+"*"+str(self.Codigo_Mesa)+"*"+str(self.Trama[2])+"*"+datetime.datetime.now().strftime("%d %b %Y")+"*"+datetime.datetime.now().strftime("%H:%M")+"*"+str(self.Row_Mstr_Paro_Maq[0])+"*")
                                        COM.Tx_Dato_Socket()
                                        COM.Conex_Cliente.close
                                        return                                
                                    else:
                                        COM.Cursor.execute("UPDATE Eventos_Produccion set Fin_Event_Prod = (?) WHERE Id_Event_Prod = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Verif_Producc[0])             
                                        COM.Cnxn.commit()
                                        COM.Cursor.execute("INSERT INTO Marcaciones(Identificacion, Codigo_Turno, Codigo_Raspi, Fecha_Ingreso) VALUES(?,?,?,?)", self.Row_Info_Empl[0], self.Trama[2], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"))           
                                        COM.Cnxn.commit()
                                        COM.Cursor.execute("INSERT INTO Eventos_Produccion(Id_Lote, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Event_Prod, Cantidad_Rasp, Operacion, Estandar, Tipo_Lect) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", self.Row_Verif_Producc[1], self.Row_Info_Empl[0], self.Trama[2], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Verif_Producc[7], self.Row_Verif_Producc[8], self.Row_Verif_Producc[9], self.Row_Verif_Producc[10])           
                                        COM.Cnxn.commit()
                                        COM.Cnxn.close
                                        COM.Dato_Tx = pickle.dumps("LI*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Empl[2])+"*"+str(self.Row_Info_Empl[3])+"*"+str(self.Codigo_Mesa)+"*"+str(self.Trama[2])+"*"+datetime.datetime.now().strftime("%d %b %Y")+"*"+datetime.datetime.now().strftime("%H:%M")+"*"+"Ninguno"+"*")
                                        COM.Tx_Dato_Socket()
                                        COM.Conex_Cliente.close
                                        return
                                else:
                                    if self.Row_Paro_Maq != None and self.Row_Paro_Maq.Fin_Paro_Maq == None:                            
                                        COM.Cursor.execute("UPDATE Paros_Maquina set Fin_Paro_Maq = (?) WHERE Id_Paro_Maq = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Paro_Maq[0])           
                                        COM.Cnxn.commit()                   
                                        COM.Cursor.execute("INSERT INTO Marcaciones(Identificacion, Codigo_Turno, Codigo_Raspi, Fecha_Ingreso) VALUES(?,?,?,?)", self.Row_Info_Empl[0], self.Trama[2], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"))           
                                        COM.Cnxn.commit()
                                        COM.Cursor.execute("INSERT INTO Paros_Maquina(Codigo_Mstr_Paro, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Id_Lote) VALUES (?, ?, ?, ?, ?, ?)", self.Row_Paro_Maq[1], self.Row_Info_Empl[0], self.Trama[2], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"),"1")
                                        COM.Cnxn.commit()
                                        COM.Cnxn.close
                                        COM.Dato_Tx = pickle.dumps("LI*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Empl[2])+"*"+str(self.Row_Info_Empl[3])+"*"+str(self.Codigo_Mesa)+"*"+str(self.Trama[2])+"*"+datetime.datetime.now().strftime("%d %b %Y")+"*"+datetime.datetime.now().strftime("%H:%M")+"*"+str(self.Row_Mstr_Paro_Maq[0])+"*")
                                        COM.Tx_Dato_Socket()
                                        COM.Conex_Cliente.close
                                        return                                
                                    else:
                                        COM.Cursor.execute("INSERT INTO Marcaciones(Identificacion, Codigo_Turno, Codigo_Raspi, Fecha_Ingreso) VALUES(?,?,?,?)", self.Row_Info_Empl[0], self.Trama[2], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"))           
                                        COM.Cnxn.commit()
                                        COM.Cnxn.close
                                        COM.Dato_Tx = pickle.dumps("LI*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Empl[2])+"*"+str(self.Row_Info_Empl[3])+"*"+str(self.Codigo_Mesa)+"*"+str(self.Trama[2])+"*"+datetime.datetime.now().strftime("%d %b %Y")+"*"+datetime.datetime.now().strftime("%H:%M")+"*"+"Ninguno"+"*")
                                        COM.Tx_Dato_Socket()
                                        COM.Conex_Cliente.close
                                        return
                            else:
                                COM.Cnxn.close
                                COM.Dato_Tx = pickle.dumps("TNE*")
                                COM.Tx_Dato_Socket()
                                COM.Conex_Cliente.close
                                return                           
            else:
                COM.Cnxn.close
                COM.Dato_Tx = pickle.dumps("NE*")
                COM.Tx_Dato_Socket()
                COM.Conex_Cliente.close
                return

#============================================================================================#

    def Est_Logueado(self):
        self.Trama = COM.Dato_Rx.split("*")
        if self.Trama[1] == "12300":
            COM.Dato_Tx = pickle.dumps("ADM*")
            COM.Tx_Dato_Socket()
            COM.Cnxn.close
            COM.Conex_Cliente.close
            return
        elif self.Trama[1] == "78900":
            COM.Dato_Tx = pickle.dumps("ADM*")
            COM.Tx_Dato_Socket()
            COM.Cnxn.close
            COM.Conex_Cliente.close
            return
        else:
            pass

        self.Dia_Anterior = datetime.datetime.now()- datetime.timedelta(days=1)
        self.Dia_Actual = datetime.datetime.now()
        if self.Dia_Anterior.month <= 9 and self.Dia_Anterior.day <= 9:
            self.Fecha_Query = ("0"+str(self.Dia_Anterior.day)+"/0"+str(self.Dia_Anterior.month)+"/"+str(self.Dia_Anterior.year)+" "+"22:00:00")
        elif self.Dia_Anterior.month <= 9:
            self.Fecha_Query = (str(self.Dia_Anterior.day)+"/0"+str(self.Dia_Anterior.month)+"/"+str(self.Dia_Anterior.year)+" "+"22:00:00")
        elif self.Dia_Anterior.day <= 9:
            self.Fecha_Query = ("0"+str(self.Dia_Anterior.day)+"/"+str(self.Dia_Anterior.month)+"/"+str(self.Dia_Anterior.year)+" "+"22:00:00")
        else:
            self.Fecha_Query = (str(self.Dia_Anterior.day)+"/"+str(self.Dia_Anterior.month)+"/"+str(self.Dia_Anterior.year)+" "+"22:00:00")

        print self.Fecha_Query 
        
        COM.Open_Conex_Sql()
        if COM.Indicad_Error == "ERROR_CNXN":
            COM.Dato_Tx = pickle.dumps("ERROR_CNXN*")
            COM.Tx_Dato_Socket()
            COM.Conex_Cliente.close
            COM.Cnxn.close
        else:        
            COM.Cursor.execute("SELECT Identificacion, Nombres, Primer_Apellido, Segundo_Apellido, Centro_Costos, Administrador from Empleados WHERE Identificacion = (?)", self.Trama[1])            
            self.Row_Info_Empl = COM.Cursor.fetchone()
            COM.Cursor.execute("SELECT Id_Lote, Codigo_Lote, Categ_Lote, Referenc_Lote, Talla_Lote, Color_Lote, Cantidad_Lote, Operac_1, Estand_1, Operac_2, Estand_2, Operac_3, Estand_3, Operac_4, Estand_4, Operac_5, Estand_5, Unidades, EAN FROM Lote WHERE Codigo_Lote = (?) ORDER BY Id_Lote DESC", self.Trama[1])
            self.Row_Producto = COM.Cursor.fetchone()
            self.EAN = "PLEGA"
            if self.Row_Producto != None:
                pass
            else:
                COM.Cursor.execute("SELECT Id_Lote, Codigo_Lote, Categ_Lote, Referenc_Lote, Talla_Lote, Color_Lote, Cantidad_Lote, Operac_1, Estand_1, Operac_2, Estand_2, Operac_3, Estand_3, Operac_4, Estand_4, Operac_5, Estand_5, Unidades, EAN FROM Lote WHERE EAN = (?) ORDER BY Id_Lote DESC", self.Trama[1])
                self.Row_Producto = COM.Cursor.fetchone()
                self.EAN = "EAN"
            COM.Cursor.execute("SELECT Codigo_Mstr_Paro, Descripcion from Mstr_Paros WHERE Codigo_Mstr_Paro = (?)", self.Trama[1])
            self.Row_Mstr_Paro_Maq = COM.Cursor.fetchone()        
            if self.Row_Info_Empl != None:
                if self.Row_Info_Empl.Administrador == True:        
                    COM.Dato_Tx = pickle.dumps("ADM*")
                    COM.Tx_Dato_Socket()
                    COM.Cnxn.close
                    COM.Conex_Cliente.close
                    return
                else:
                    COM.Cursor.execute("SELECT TOP(1) Id_Marcacion, Identificacion, Codigo_Turno, Codigo_Raspi, Fecha_Ingreso, Fecha_Salida FROM Marcaciones WHERE Identificacion = (?) AND Codigo_Raspi = (?) ORDER BY Id_Marcacion DESC", self.Row_Info_Empl[0], self.Codigo_Mesa)
                    self.Row_Marcacion = COM.Cursor.fetchone()
                    COM.Cursor.execute("SELECT TOP(1) Id_Event_Prod, Id_Lote, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Event_Prod, Fin_Event_Prod, Cantidad_Rasp, Operacion, Estandar, Tipo_Lect FROM Eventos_Produccion WHERE Identificacion = (?) AND Codigo_Raspi = (?) ORDER BY Id_Event_Prod DESC", self.Row_Info_Empl[0], self.Codigo_Mesa)            
                    self.Row_Verif_Producc = COM.Cursor.fetchone()
                    COM.Cursor.execute("SELECT TOP(1) Id_Paro_Maq, Codigo_Mstr_Paro, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Fin_Paro_Maq FROM Paros_Maquina WHERE Identificacion = (?) AND Codigo_Raspi = (?) ORDER BY Id_Paro_Maq desc", self.Row_Info_Empl[0], self.Codigo_Mesa)
                    self.Row_Paro_Maq = COM.Cursor.fetchone()
                    if self.Row_Paro_Maq != None and self.Row_Paro_Maq.Fin_Paro_Maq == None:  
                        COM.Cursor.execute("SELECT Descripcion from Mstr_Paros WHERE Codigo_Mstr_Paro = (?)", self.Row_Paro_Maq[1])
                        self.Row_Mstr_Paro_Maq = COM.Cursor.fetchone()                    
                        self.Est_Maquina = self.Row_Mstr_Paro_Maq[0]
                    else:
                        self.Est_Maquina = "Maq. Activa"
                    if self.Row_Marcacion != None and self.Row_Marcacion.Fecha_Salida == None and self.Row_Marcacion.Identificacion == self.Row_Info_Empl.Identificacion:                  
                        if self.Row_Verif_Producc != None and self.Row_Verif_Producc.Fin_Event_Prod == None:
                            if self.Row_Paro_Maq != None and self.Row_Paro_Maq.Fin_Paro_Maq == None:
                                COM.Cursor.execute("UPDATE Paros_Maquina set Fin_Paro_Maq = (?) WHERE Id_Paro_Maq = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Paro_Maq[0])           
                                COM.Cnxn.commit()
                                COM.Cursor.execute("DELETE FROM Eventos_Produccion WHERE Id_Event_Prod = (?)", self.Row_Verif_Producc[0])
                                COM.Cnxn.commit()
                                COM.Cursor.execute("UPDATE Marcaciones set Fecha_Salida = (?) WHERE Id_Marcacion = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Marcacion[0])           
                                COM.Cnxn.commit()
                            else:
                                COM.Cursor.execute("DELETE FROM Eventos_Produccion WHERE Id_Event_Prod = (?)", self.Row_Verif_Producc[0])
                                COM.Cnxn.commit()
                                COM.Cursor.execute("UPDATE Marcaciones set Fecha_Salida = (?) WHERE Id_Marcacion = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Marcacion[0])           
                                COM.Cnxn.commit()
                            COM.Cnxn.close
                            if self.Row_Efici_Oper != None:                          
                                COM.Dato_Tx = pickle.dumps("LO*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Empl[2])+"*"+str(self.Row_Info_Empl[3])+"*"+datetime.datetime.now().strftime("%d %b %Y")+"*"+datetime.datetime.now().strftime("%H:%M")+"*"+str(self.Est_Maquina)+"*"+str(self.Row_Efici_Oper[3])+"*")                            
                                COM.Tx_Dato_Socket()
                                COM.Conex_Cliente.close
                                return
                            else:
                                COM.Dato_Tx = pickle.dumps("LO*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Empl[2])+"*"+str(self.Row_Info_Empl[3])+"*"+datetime.datetime.now().strftime("%d %b %Y")+"*"+datetime.datetime.now().strftime("%H:%M")+"*"+str(self.Est_Maquina)+"*"+"0"+"*")                            
                                COM.Tx_Dato_Socket()
                                COM.Conex_Cliente.close
                                return                            
                        else:
                            if self.Row_Paro_Maq != None and self.Row_Paro_Maq.Fin_Paro_Maq == None:
                                COM.Cursor.execute("UPDATE Paros_Maquina set Fin_Paro_Maq = (?) WHERE Id_Paro_Maq = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Paro_Maq[0])           
                                COM.Cnxn.commit()
                                COM.Cursor.execute("UPDATE Marcaciones set Fecha_Salida = (?) WHERE Id_Marcacion = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Marcacion[0])           
                                COM.Cnxn.commit()
                            else:
                                COM.Cursor.execute("UPDATE Marcaciones set Fecha_Salida = (?) WHERE Id_Marcacion = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Marcacion[0])           
                                COM.Cnxn.commit()
                            COM.Cnxn.close
                            if self.Row_Efici_Oper != None:                        
                                COM.Dato_Tx = pickle.dumps("LO*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Empl[2])+"*"+str(self.Row_Info_Empl[3])+"*"+datetime.datetime.now().strftime("%d %b %Y")+"*"+datetime.datetime.now().strftime("%H:%M")+"*"+str(self.Est_Maquina)+"*"+str(self.Row_Efici_Oper[3])+"*")                            
                                COM.Tx_Dato_Socket()
                                COM.Conex_Cliente.close
                                return
                            else:
                                COM.Dato_Tx = pickle.dumps("LO*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Empl[2])+"*"+str(self.Row_Info_Empl[3])+"*"+datetime.datetime.now().strftime("%d %b %Y")+"*"+datetime.datetime.now().strftime("%H:%M")+"*"+str(self.Est_Maquina)+"*"+"0"+"*")                            
                                COM.Tx_Dato_Socket()
                                COM.Conex_Cliente.close
                                return
                    else:
                        COM.Cnxn.close
                        COM.Dato_Tx = pickle.dumps("LOGOK*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                        return                    
            elif self.Row_Producto != None:                
                COM.Cursor.execute("SELECT TOP(1) Id_Event_Prod, Id_Lote, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Event_Prod, Fin_Event_Prod, Cantidad_Rasp, Operacion, Estandar, Tipo_Lect FROM Eventos_Produccion WHERE Codigo_Raspi = (?) and Ini_Event_Prod >= (?)  ORDER BY Id_Event_Prod DESC", self.Codigo_Mesa, self.Fecha_Query)            
                self.Row_Verif_Producc = COM.Cursor.fetchone()
                COM.Cursor.execute("SELECT TOP(1) Id_Paro_Maq, Codigo_Mstr_Paro, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Fin_Paro_Maq FROM Paros_Maquina WHERE Codigo_Raspi = (?) AND Ini_Paro_Maq >= (?) ORDER BY Id_Paro_Maq desc", self.Codigo_Mesa, self.Fecha_Query)
                self.Row_Paro_Maq = COM.Cursor.fetchone()          
                if self.Row_Verif_Producc != None and  self.Row_Verif_Producc.Fin_Event_Prod == None and self.Trama[2] == "0":
                    if self.Row_Verif_Producc.Id_Lote == self.Row_Producto.Id_Lote:
                        if self.Row_Paro_Maq != None and self.Row_Paro_Maq.Fin_Paro_Maq == None:
                            COM.Cursor.execute("UPDATE Paros_Maquina set Fin_Paro_Maq = (?) WHERE Id_Paro_Maq = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Paro_Maq[0])           
                            COM.Cnxn.commit()
                        else:
                            pass

                        self.Row_Verif_Producc[7]=int(self.Row_Verif_Producc[7])+int(self.Row_Producto.Unidades)-1
                        self.Unidades_Oper = self.Row_Verif_Producc[7]
                        COM.Cursor.execute("UPDATE Eventos_Produccion set Fin_Event_Prod = (?), Cantidad_Rasp = (?), Tipo_Lect = (?) WHERE Id_Event_Prod = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Verif_Producc[7], self.EAN, self.Row_Verif_Producc[0])           
                        COM.Cnxn.commit()
                        self.Row_Verif_Producc[7]=int(self.Row_Verif_Producc[7])+1
                        COM.Cursor.execute("INSERT INTO Eventos_Produccion(Id_Lote, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Event_Prod, Cantidad_Rasp, Operacion, Estandar, Tipo_Lect) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", self.Row_Verif_Producc[1], self.Row_Verif_Producc[2], self.Row_Verif_Producc[3], self.Row_Verif_Producc[4], datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), str(self.Row_Verif_Producc[7]), self.Row_Verif_Producc[8], self.Row_Verif_Producc[9], self.EAN)           
                        COM.Cnxn.commit()
                        COM.Cursor.execute("SELECT TOP(1) Id_Marcacion, Identificacion, Codigo_Turno, Codigo_Raspi, Fecha_Ingreso, Fecha_Salida, Marc_Modificada FROM Marcaciones WHERE Codigo_Raspi = (?) ORDER BY Id_Marcacion DESC", self.Codigo_Mesa)            
                        self.Row_Verif_Logueo = COM.Cursor.fetchone()
                        CNXNS.Calcula_Eficienc()                    
                        COM.Cnxn.close                         
                        if CNXNS.Indic_Error_Efic == "ERROR_EFIC":
                            COM.Dato_Tx = pickle.dumps("ERROR_CNXN*")
                            COM.Tx_Dato_Socket()
                            COM.Conex_Cliente.close
                        else:                         
                            if self.Row_Efici_Oper != None:
                                COM.Dato_Tx = pickle.dumps("ALMACMARC*"+str(self.Unidades_Oper)+"*"+str(self.Row_Efici_Oper[3])+"*"+str(self.Row_Efici_Oper[4])+"*")
                                COM.Tx_Dato_Socket()
                                COM.Conex_Cliente.close
                                return
                            else:
                                COM.Dato_Tx = pickle.dumps("ALMACMARC*"+str(self.Unidades_Oper)+"*"+"0"+"*"+"0"+"*")
                                COM.Tx_Dato_Socket()
                                COM.Conex_Cliente.close
                                return                        
                    else:
                        COM.Cnxn.close
                        COM.Dato_Tx = pickle.dumps("FINLOTPROC*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                        return                
                elif self.Trama[2] >= "1" and self.Trama[2] <= "5":
                    if self.Row_Verif_Producc != None and  self.Row_Verif_Producc.Fin_Event_Prod == None:                
                        COM.Cursor.execute("DELETE FROM Eventos_Produccion WHERE Id_Event_Prod = (?)", self.Row_Verif_Producc[0])
                        COM.Cnxn.commit()
                        self.Sum_Cantidad = self.Row_Verif_Producc[7]
                    else:
                        COM.Cursor.execute("SELECT TOP(1) Cantidad_Rasp FROM Eventos_Produccion WHERE Codigo_Raspi = (?) AND Id_Lote = (?) and Ini_Event_Prod >= (?) ORDER BY Id_Event_Prod DESC", self.Codigo_Mesa, self.Row_Producto[0], self.Fecha_Query)            
                        self.Cantid_Acumulada = COM.Cursor.fetchone()
                        if self.Cantid_Acumulada == None:                        
                            self.Sum_Cantidad = "1"
                        else:
                            self.Sum_Cantidad = self.Cantid_Acumulada[0] + 1                    
                    if self.Trama[2] == "1":        
                        self.Operac=self.Row_Producto[7]
                        self.Estand=self.Row_Producto[8]
                    elif self.Trama[2] == "2":        
                        self.Operac=self.Row_Producto[9]
                        self.Estand=self.Row_Producto[10]
                    elif self.Trama[2] == "3":        
                        self.Operac=self.Row_Producto[11]
                        self.Estand=self.Row_Producto[12]
                    elif self.Trama[2] == "4":        
                        self.Operac=self.Row_Producto[13]
                        self.Estand=self.Row_Producto[14]
                    else:        
                        self.Operac=self.Row_Producto[15]
                        self.Estand=self.Row_Producto[16]
                    COM.Cursor.execute("SELECT TOP(1) Identificacion, Codigo_Turno FROM Marcaciones WHERE Codigo_Raspi = (?) ORDER BY Id_Marcacion DESC", self.Codigo_Mesa)
                    self.Row_Marcacion = COM.Cursor.fetchone()
                    COM.Cursor.execute("SELECT TOP(1) Id_Paro_Maq, Codigo_Mstr_Paro, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Fin_Paro_Maq FROM Paros_Maquina WHERE Codigo_Raspi = (?)ORDER BY Id_Paro_Maq desc", self.Codigo_Mesa)
                    self.Row_Paro_Maq = COM.Cursor.fetchone()
                    if self.Row_Paro_Maq != None and  self.Row_Paro_Maq.Fin_Paro_Maq == None:
                        COM.Cursor.execute("UPDATE Paros_Maquina set Fin_Paro_Maq = (?) WHERE Id_Paro_Maq = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Paro_Maq[0])           
                        COM.Cnxn.commit()        
                        COM.Cursor.execute("INSERT INTO Eventos_Produccion(Id_Lote, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Event_Prod, Cantidad_Rasp, Operacion, Estandar, Tipo_Lect) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", self.Row_Producto[0], self.Row_Marcacion[0], self.Row_Marcacion[1], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Sum_Cantidad, self.Operac, self.Estand, self.EAN)           
                        COM.Cnxn.commit()
                    else:
                        COM.Cursor.execute("INSERT INTO Eventos_Produccion(Id_Lote, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Event_Prod, Cantidad_Rasp, Operacion, Estandar, Tipo_Lect) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", self.Row_Producto[0], self.Row_Marcacion[0], self.Row_Marcacion[1], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Sum_Cantidad, self.Operac, self.Estand, self.EAN)           
                        COM.Cnxn.commit()
                    COM.Cnxn.close
                    if self.EAN != "EAN":
                        COM.Dato_Tx = pickle.dumps("PRODI*"+str(self.Row_Producto[1])+"*"+str(self.Row_Producto[3])+"*"+str(self.Row_Producto[6])+"*"+datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")+"*"+str(self.Operac)+"*"+str(self.Estand)+"*"+str(self.Row_Producto.Unidades)+"*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                        return
                    else:
                        COM.Dato_Tx = pickle.dumps("PRODI*"+str(self.Row_Producto[18])+"*"+str(self.Row_Producto[3])+"*"+str(self.Row_Producto[6])+"*"+datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")+"*"+str(self.Operac)+"*"+str(self.Estand)+"*"+str(self.Row_Producto.Unidades)+"*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                        return                    
                else:
                    COM.Cnxn.close
                    if self.EAN != "EAN":
                        COM.Dato_Tx = pickle.dumps("STDR*"+str(self.Row_Producto[1])+"*"+str(self.Row_Producto[3])+"*"+str(self.Row_Producto[7])+"*"+str(self.Row_Producto[8])+"*"+str(self.Row_Producto[9])+"*"+str(self.Row_Producto[10])+"*"+str(self.Row_Producto[11])+"*"+str(self.Row_Producto[12])+"*"+str(self.Row_Producto[13])+"*"+str(self.Row_Producto[14])+"*"+str(self.Row_Producto[15])+"*"+str(self.Row_Producto[16])+"*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                        return
                    else:
                        COM.Dato_Tx = pickle.dumps("STDR*"+str(self.Row_Producto[18])+"*"+str(self.Row_Producto[3])+"*"+str(self.Row_Producto[7])+"*"+str(self.Row_Producto[8])+"*"+str(self.Row_Producto[9])+"*"+str(self.Row_Producto[10])+"*"+str(self.Row_Producto[11])+"*"+str(self.Row_Producto[12])+"*"+str(self.Row_Producto[13])+"*"+str(self.Row_Producto[14])+"*"+str(self.Row_Producto[15])+"*"+str(self.Row_Producto[16])+"*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                        return
                        
            elif self.Row_Mstr_Paro_Maq != None:
                COM.Cursor.execute("SELECT TOP(1) Id_Paro_Maq, Codigo_Mstr_Paro, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Fin_Paro_Maq FROM Paros_Maquina WHERE Codigo_Raspi = (?)ORDER BY Id_Paro_Maq desc", self.Codigo_Mesa)
                self.Row_Paro_Maq = COM.Cursor.fetchone()
                if self.Row_Paro_Maq != None and self.Row_Paro_Maq.Fin_Paro_Maq == None:
                    if self.Row_Paro_Maq.Codigo_Mstr_Paro != self.Row_Mstr_Paro_Maq.Codigo_Mstr_Paro:
                        COM.Cursor.execute("UPDATE Paros_Maquina set Fin_Paro_Maq = (?) WHERE Id_Paro_Maq = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Paro_Maq[0])           
                        COM.Cnxn.commit()
                        COM.Cursor.execute("SELECT TOP(1) Id_Marcacion, Identificacion, Codigo_Turno, Codigo_Raspi, Fecha_Ingreso, Fecha_Salida, Marc_Modificada FROM Marcaciones WHERE Codigo_Raspi = (?) ORDER BY Id_Marcacion DESC", self.Codigo_Mesa)            
                        self.Row_Verif_Logueo = COM.Cursor.fetchone()
                        COM.Cursor.execute("SELECT TOP(1) Id_Lote, Fin_Event_Prod FROM Eventos_Produccion WHERE Codigo_Raspi = (?) ORDER BY Id_Event_Prod DESC", self.Codigo_Mesa)            
                        self.Row_Verif_Producc = COM.Cursor.fetchone()
                        if self.Row_Verif_Producc != None and  self.Row_Verif_Producc.Fin_Event_Prod == None:                                
                            COM.Cursor.execute("INSERT INTO Paros_Maquina(Codigo_Mstr_Paro, Identificacion,Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Id_Lote) VALUES (?, ?, ?, ?, ?, ?)", self.Row_Mstr_Paro_Maq[0], self.Row_Verif_Logueo[1], self.Row_Verif_Logueo[2], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Verif_Producc[0])           
                            COM.Cnxn.commit()                        
                        else:
                            COM.Cursor.execute("INSERT INTO Paros_Maquina(Codigo_Mstr_Paro, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Id_Lote) VALUES (?, ?, ?, ?, ?, ?)", self.Row_Mstr_Paro_Maq[0], self.Row_Verif_Logueo[1], self.Row_Verif_Logueo[2], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), "1")           
                            COM.Cnxn.commit()
                        
                        COM.Cnxn.close
                        COM.Dato_Tx = pickle.dumps("MPARO*"+str(self.Row_Mstr_Paro_Maq[0])+"*"+str(self.Row_Mstr_Paro_Maq[1])+"*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                        return
                    else:
                        COM.Cursor.execute("UPDATE Paros_Maquina set Fin_Paro_Maq = (?) WHERE Id_Paro_Maq = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Paro_Maq[0])           
                        COM.Cnxn.commit()
                        COM.Cnxn.close
                        COM.Dato_Tx = pickle.dumps("PPROC*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                        return
                else:
                    COM.Cursor.execute("SELECT TOP(1) Id_Marcacion, Identificacion, Codigo_Turno, Codigo_Raspi, Fecha_Ingreso, Fecha_Salida, Marc_Modificada FROM Marcaciones WHERE Codigo_Raspi = (?) ORDER BY Id_Marcacion DESC", self.Codigo_Mesa)            
                    self.Row_Verif_Logueo = COM.Cursor.fetchone()
                    COM.Cursor.execute("SELECT TOP(1) Id_Lote, Fin_Event_Prod, Id_Event_Prod  FROM Eventos_Produccion WHERE Codigo_Raspi = (?) ORDER BY Id_Event_Prod DESC", self.Codigo_Mesa)
                    self.Row_Verif_Producc = COM.Cursor.fetchone()       
                    if self.Row_Verif_Producc != None and  self.Row_Verif_Producc.Fin_Event_Prod == None:                                
                        COM.Cursor.execute("INSERT INTO Paros_Maquina(Codigo_Mstr_Paro, Identificacion,Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Id_Lote) VALUES (?, ?, ?, ?, ?, ?)", self.Row_Mstr_Paro_Maq[0], self.Row_Verif_Logueo[1], self.Row_Verif_Logueo[2], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Verif_Producc[0])           
                        COM.Cnxn.commit()
                    else:
                        COM.Cursor.execute("INSERT INTO Paros_Maquina(Codigo_Mstr_Paro, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Id_Lote) VALUES (?, ?, ?, ?, ?, ?)", self.Row_Mstr_Paro_Maq[0], self.Row_Verif_Logueo[1], self.Row_Verif_Logueo[2], self.Codigo_Mesa, datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), "1")           
                        COM.Cnxn.commit()
                    COM.Cnxn.close
                    COM.Dato_Tx = pickle.dumps("MPARO*"+str(self.Row_Mstr_Paro_Maq[0])+"*"+str(self.Row_Mstr_Paro_Maq[1])+"*")
                    COM.Tx_Dato_Socket()
                    COM.Conex_Cliente.close
                    return                                  
            elif self.Trama[1] == "202020":
                COM.Cursor.execute("SELECT TOP(1) Id_Event_Prod, Id_Lote, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Event_Prod, Fin_Event_Prod, Cantidad_Rasp, Operacion, Estandar, Tipo_Lect FROM Eventos_Produccion WHERE Codigo_Raspi = (?) ORDER BY Id_Event_Prod DESC", self.Codigo_Mesa)            
                self.Row_Verif_Producc = COM.Cursor.fetchone()
                if self.Row_Verif_Producc != None and  self.Row_Verif_Producc.Fin_Event_Prod == None:
                    COM.Cursor.execute("DELETE FROM Eventos_Produccion WHERE Id_Event_Prod = (?)", self.Row_Verif_Producc[0])
                    COM.Cnxn.commit()
                    COM.Cursor.execute("SELECT Codigo_Lote, Referenc_Lote, EAN FROM Lote WHERE Id_Lote = (?)", self.Row_Verif_Producc[1])
                    self.Row_Producto = COM.Cursor.fetchone()
                    COM.Cnxn.close
                    if self.Row_Verif_Producc.Tipo_Lect != "EAN":
                        self.Row_Verif_Producc[7]=self.Row_Verif_Producc[7]-1
                        COM.Dato_Tx = pickle.dumps("PRODF*"+str(self.Row_Producto[0])+"*"+str(self.Row_Producto[1])+"*"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"*"+str(self.Row_Verif_Producc[7])+"*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                    else:
                        self.Row_Verif_Producc[7]=self.Row_Verif_Producc[7]-1
                        COM.Dato_Tx = pickle.dumps("PRODF*"+str(self.Row_Producto[2])+"*"+str(self.Row_Producto[1])+"*"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"*"+str(self.Row_Verif_Producc[7])+"*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close                    
                else:
                    COM.Cnxn.close
                    COM.Dato_Tx = pickle.dumps("NOLOTINIC*")
                    COM.Tx_Dato_Socket()
                    COM.Conex_Cliente.close                
            elif self.Trama[1] == "101010":
                COM.Cursor.execute("SELECT TOP(1) Id_Event_Prod, Id_Lote, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Event_Prod, Fin_Event_Prod, Cantidad_Rasp, Operacion, Estandar, Tipo_Lect FROM Eventos_Produccion WHERE Codigo_Raspi = (?) ORDER BY Id_Event_Prod DESC", self.Codigo_Mesa)            
                self.Row_Verif_Producc = COM.Cursor.fetchone()
                if self.Row_Verif_Producc != None and  self.Row_Verif_Producc.Fin_Event_Prod == None:
                    COM.Cursor.execute("SELECT Id_Lote, Codigo_Lote, Categ_Lote, Referenc_Lote, Talla_Lote, Color_Lote, Cantidad_Lote, Operac_1, Estand_1, Operac_2, Estand_2, Operac_3, Estand_3, Operac_4, Estand_4, Operac_5, Estand_5, EAN FROM Lote WHERE Id_Lote = (?)", self.Row_Verif_Producc[1])
                    self.Row_Producto = COM.Cursor.fetchone()
                    COM.Cnxn.close
                    if self.Row_Verif_Producc.Tipo_Lect != "EAN":
                        COM.Dato_Tx = pickle.dumps("STDR*"+str(self.Row_Producto[1])+"*"+str(self.Row_Producto[3])+"*"+str(self.Row_Producto[7])+"*"+str(self.Row_Producto[8])+"*"+str(self.Row_Producto[9])+"*"+str(self.Row_Producto[10])+"*"+str(self.Row_Producto[11])+"*"+str(self.Row_Producto[12])+"*"+str(self.Row_Producto[13])+"*"+str(self.Row_Producto[14])+"*"+str(self.Row_Producto[15])+"*"+str(self.Row_Producto[16])+"*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                        return
                    else:
                        COM.Dato_Tx = pickle.dumps("STDR*"+str(self.Row_Producto[17])+"*"+str(self.Row_Producto[3])+"*"+str(self.Row_Producto[7])+"*"+str(self.Row_Producto[8])+"*"+str(self.Row_Producto[9])+"*"+str(self.Row_Producto[10])+"*"+str(self.Row_Producto[11])+"*"+str(self.Row_Producto[12])+"*"+str(self.Row_Producto[13])+"*"+str(self.Row_Producto[14])+"*"+str(self.Row_Producto[15])+"*"+str(self.Row_Producto[16])+"*")
                        COM.Tx_Dato_Socket()
                        COM.Conex_Cliente.close
                        return
                else:
                    COM.Cnxn.close
                    COM.Dato_Tx = pickle.dumps("NOLOTINIC*")
                    COM.Tx_Dato_Socket()
                    COM.Conex_Cliente.close
            else:
                COM.Cnxn.close
                COM.Dato_Tx = pickle.dumps("NE*")
                COM.Tx_Dato_Socket()
                COM.Conex_Cliente.close
                return

#============================================================================================#

    def Deslog_Usuario(self):
        self.Trama=COM.Dato_Rx.split("*")
        COM.Open_Conex_Sql()
        if COM.Indicad_Error == "ERROR_CNXN":
            COM.Dato_Tx = pickle.dumps("ERROR_CNXN*")
            COM.Tx_Dato_Socket()
            COM.Conex_Cliente.close
            COM.Cnxn.close
        else:           
            COM.Cursor.execute("SELECT TOP(1) Id_Marcacion, Identificacion, Codigo_Turno, Codigo_Raspi, Fecha_Ingreso, Fecha_Salida, Marc_Modificada FROM Marcaciones WHERE Codigo_Raspi = (?) ORDER BY Id_Marcacion DESC", self.Codigo_Mesa)            
            self.Row_Verif_Logueo = COM.Cursor.fetchone()
            if self.Row_Verif_Logueo != None and self.Row_Verif_Logueo.Fecha_Salida == None:                
                COM.Cursor.execute("SELECT Identificacion, Nombres, Primer_Apellido, Segundo_Apellido, Centro_Costos, Administrador from Empleados WHERE Identificacion = (?)", self.Row_Verif_Logueo[1])            
                self.Row_Info_Empl = COM.Cursor.fetchone()       
                COM.Cursor.execute("UPDATE Marcaciones set Fecha_Salida = (?) WHERE Id_Marcacion = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Verif_Logueo[0])           
                COM.Cnxn.commit()
                COM.Cursor.execute("UPDATE Marcaciones set Marc_Modificada = (?) WHERE Id_Marcacion = (?)", "True", self.Row_Verif_Logueo[0])           
                COM.Cnxn.commit()
                COM.Cursor.execute("INSERT INTO Eventos_Admin(Id_Autoriza, Id_Marcacion, Descripc_Evento, Fecha_Ini_Event) VALUES (?, ?, ?, ?)", self.Trama[1], self.Row_Verif_Logueo[0], "Logout", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"))           
                COM.Cnxn.commit()
                COM.Cursor.execute("SELECT TOP(1) Id_Event_Prod, Id_Lote, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Event_Prod, Fin_Event_Prod, Cantidad_Rasp, Eficiencia FROM Eventos_Produccion WHERE Codigo_Raspi = (?) ORDER BY Id_Event_Prod DESC", self.Codigo_Mesa)            
                self.Row_Verif_Producc = COM.Cursor.fetchone()                                  
                if self.Row_Verif_Producc != None and self.Row_Verif_Producc.Fin_Event_Prod == None:                        
                    COM.Cursor.execute("DELETE Eventos_Produccion WHERE Id_Event_Prod = (?)", self.Row_Verif_Producc[0])
                    COM.Cnxn.commit()
                else:
                    pass
                COM.Cursor.execute("SELECT TOP(1) Id_Paro_Maq, Codigo_Mstr_Paro, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Fin_Paro_Maq FROM Paros_Maquina WHERE Codigo_Raspi = (?)ORDER BY Id_Paro_Maq desc", self.Codigo_Mesa)
                self.Row_Paro_Maq = COM.Cursor.fetchone()
                if self.Row_Paro_Maq != None and self.Row_Paro_Maq.Fin_Paro_Maq == None:                            
                    COM.Cursor.execute("UPDATE Paros_Maquina set Fin_Paro_Maq = (?) WHERE Id_Paro_Maq = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Row_Paro_Maq[0])           
                    COM.Cnxn.commit()
                else:
                    pass
                
                COM.Dato_Tx = pickle.dumps("ADMLGT*"+str(self.Row_Info_Empl[0])+"*"+str(self.Row_Info_Empl[1])+"*"+str(self.Row_Info_Empl[2])+"*"+str(self.Row_Info_Empl[3])+"*"+str(self.Row_Info_Empl[4])+"*"+str(self.Row_Info_Empl[5])+"*"+str(self.Codigo_Mesa)+"*"+str(self.Row_Verif_Logueo[2])+"*"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"*"+str("Generico")+"*"+str(0)+"*")
                COM.Tx_Dato_Socket()
                COM.Conex_Cliente.close
                COM.Cnxn.close
                return

            else:
                COM.Dato_Tx = pickle.dumps("NE*")
                COM.Tx_Dato_Socket()
                COM.Conex_Cliente.close
                COM.Cnxn.close
                return
       
#============================================================================================#    

class Hilo_Cierre_Turno(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.Hora_Fin_Turno = 0
        self.Turno_Cierre = 0
        self.Turno_Apertur = 0
        self.Verif_Producc_CT = 0
        self.Paro_Maq_CT = 0
        self.Indicad_Cnxn = 0
        self.Reg_Cierre_Turno = 0
        self.Reg_Fin_loguin = 0
        self.Reg_Fin_Event_Prod = 0
        self.Reg_Fin_Par_Maq = 0
        self.Row_Marc_Empleado = 0
        self.Captura_Hora = 0
        self.Row_Marc_Empl_C_Tur = 0
        
#============================================================================================#

    def run(self):
        while 1:
            time.sleep(10)
            self.Captura_Hora = time.strftime("%H:%M")
            while 1:
                if CLSTURNO.Indicad_Cnxn == 0:                            
                    CLSTURNO.Indicad_Cnxn = 2
                    break
                else:
                    time.sleep(.100)         
            COM.Open_Conex_Sql()
            COM.Cursor.execute("SELECT Marc.Id_Marcacion, Marc.Identificacion, Marc.Codigo_Turno, Marc.Codigo_Raspi, T_Empl.Hora_Inicio, T_Empl.Hora_Final, T_Empl.Tiempo_Turno_Minutos, Marc.Fecha_Ingreso, CASE WHEN (T_Empl.Hora_Inicio != '00:00:00') THEN ((DATEDIFF(ss,T_Empl.Hora_Inicio,CONVERT(varchar(25), GETDATE(), 108)))/60) ELSE ((DATEDIFF(ss,CONVERT(varchar(25), Marc.Fecha_Ingreso, 108),CONVERT(varchar(25), GETDATE(), 108)))/60) END AS Tiempo_Logueo FROM Marcaciones as Marc INNER JOIN Turno_Empleado as T_Empl ON Marc.Codigo_Turno = T_Empl.Codigo_Turno WHERE (Fecha_Salida IS NULL) ORDER BY Id_Marcacion")
            self.Row_Marc_Empl_C_Tur = COM.Cursor.fetchall()
            if self.Row_Marc_Empl_C_Tur != None:
                for self.Reg_Cierre_Turno in self.Row_Marc_Empl_C_Tur:
                    if (self.Reg_Cierre_Turno[6] - self.Reg_Cierre_Turno[8]) <= 0:
                        print ""
                        print "Cierre Turno:"
                        print "Identif: ",self.Reg_Cierre_Turno[1]," Mesa: ", self.Reg_Cierre_Turno[3]," Turno: ", self.Reg_Cierre_Turno[2]
                        COM.Cursor.execute("SELECT Id_Paro_Maq, Codigo_Mstr_Paro, Identificacion, Codigo_Turno, Codigo_Raspi, Ini_Paro_Maq, Fin_Paro_Maq, Id_Lote FROM Paros_Maquina WHERE Fin_Paro_Maq is NULL AND Identificacion = (?) AND Codigo_Turno = (?) AND Codigo_Raspi = (?) ORDER BY Id_Paro_Maq ASC", self.Reg_Cierre_Turno[1], self.Reg_Cierre_Turno[2],self.Reg_Cierre_Turno[3])        
                        self.Paro_Maq_CT = COM.Cursor.fetchall()                   
                        if self.Paro_Maq_CT != None:
                            for self.Reg_Fin_Par_Maq in self.Paro_Maq_CT:
                                COM.Cursor.execute("UPDATE Paros_Maquina set Fin_Paro_Maq = (?) WHERE Id_Paro_Maq = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Reg_Fin_Par_Maq[0])           
                                COM.Cnxn.commit()
                        else:
                            pass
                        COM.Cursor.execute("SELECT Id_Event_Prod FROM Eventos_Produccion WHERE Fin_Event_Prod is NULL AND Identificacion = (?) AND Codigo_Turno = (?) AND Codigo_Raspi = (?) ORDER BY Id_Event_Prod", self.Reg_Cierre_Turno[1], self.Reg_Cierre_Turno[2],self.Reg_Cierre_Turno[3])            
                        self.Verif_Producc_CT = COM.Cursor.fetchall()
                        if self.Verif_Producc_CT != None:
                            for self.Reg_Fin_Event_Prod in self.Verif_Producc_CT:
                                COM.Cursor.execute("DELETE FROM Eventos_Produccion WHERE Id_Event_Prod = (?)", self.Reg_Fin_Event_Prod[0])
                                COM.Cnxn.commit()
                        else:
                            pass
                        COM.Cursor.execute("UPDATE Marcaciones set Fecha_Salida = (?) WHERE Id_Marcacion = (?)", datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), self.Reg_Cierre_Turno[0])
                        COM.Cnxn.commit()
                    else:
                        pass
            else:
                pass                          
       
            COM.Cnxn.close
            CLSTURNO.Indicad_Cnxn = 0              
            
#=#============================================================================================#

COM = Comunicaciones()
COM.Open_Socket()
CNXNS = Hilo_Conexiones()
CNXNS.start()
CLSTURNO = Hilo_Cierre_Turno()
CLSTURNO.start()
while 1:
    pass

#============================================================================================#
    



    
