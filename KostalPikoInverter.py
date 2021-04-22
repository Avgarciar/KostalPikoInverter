# Modulo para la lectura de informacion de valores de un INVERSOR KOSTAL PIKO MP PLUS conectado a un Smart Meter

# Planificación


# Libreria de acceso a URL
from urllib.request import urlopen

# Libreria de Lectura de XML
from lxml import etree

import ssl
import sys
import time
import json
import os

# Librería de acceso a borker MQTT
import paho.mqtt.client



#Creamos la clase Inversorkostarl con los atributos de energía y los métodos


class InversorKostal():
       
    def __init__(self):

        import configparser

#         Leemos el archivo de configuración
        config = configparser.ConfigParser()
        absFilePath = os.path.abspath(__file__)
        self.path, filename = os.path.split(absFilePath)
        config.read(self.path + '/KostalPikoInverter.ini')
        self.InversorIP = config['DEFAULT']['InversorIP']
        self.BrokerMqttIP= config['DEFAULT']['BrokerMqttIP']
        self.BrokerPort= int(config['DEFAULT']['BrokerPort'])
        self.BrokerUser= config['DEFAULT']['BrokerUser']
        self.BrokerPassWord= config['DEFAULT']['BrokerPassWord']
        self.FrecuenciaLectura= float(config['DEFAULT']['FrecuenciaLectura'])
        self.PublicarMqtt= config['DEFAULT'].getboolean ('PublicarMqtt')
        self.TopicMQTT= config['DEFAULT']['TopicMQTT']
        self.PublicarFreeDS= config['DEFAULT'].getboolean('PublicarFreeDS')
        self.TopicSolar= config['DEFAULT']['TopicSolar']
        self.TopicMeter= config['DEFAULT']['TopicMeter']
        self.Debug = config['DEFAULT'].getboolean ('Debug')
        self.PrintDebug ("ruta de ejecución: " + absFilePath)
       
        
        self.AC_Voltage = 0
        self.AC_Current = 0
        self.AC_Power = 0
        self.AC_Power_fast= 0
        self.AC_Frequency= 0
        self.DC_Voltage1 = 0
        self.DC_Voltage2 = 0
        self.DC_Current1 = 0
        self.DC_Current2 = 0
        self.LINK_Voltage = 0
        self.GridConsumedPower = 0
        self.GridPower= 0
        self.GridInjectedPower = 0
        self.OwnConsumedPower = 0
        self.Derating = 0
        self.ConectadoInversor = False
        self.ConectadoMQTT = False
        self.ClienteMQTT=  paho.mqtt.client.Client("piko_python")
        if self.BrokerUser != '':
            self.ClienteMQTT.username_pw_set(self.BrokerUser, password=self.BrokerPassWord )
        
        #Cadena de conexión al Inveror
        CadenaConexion = "http://" + self.InversorIP + "/measurements.xml"
        
        self.PrintDebug ("Creado el Objeto Inversor")
        
        #Conectamos con el Inversor
        contador=0
        while not (self.ConectadoInversor):
        
            contador=contador+1
            try:
                Documento = urlopen (CadenaConexion).read()
                self.ConectadoInversor = True
                self.PrintDebug ('Conectado al Inversor en: '+ CadenaConexion)
        
            except:
                #Error al conectar al inversor
                self.ConectadoInversor = False      
                self.PrintDebug ('Intento: ' + str (contador) + '. Error al conectar al Inversor Kostal en la dirección: ' + str (self.InversorIP))
                e = sys.exc_info()[1]
                textoerror= str (e.args[0])
                self.PrintDebug(textoerror)
                time.sleep (5)

        #Conectamos MQTT
        if self.PublicarMqtt or self.PublicarFreeDS:
            contador=0
            while not (self.ConectadoMQTT):
                contador=contador + 1
                try:
                    self.ClienteMQTT.connect (self.BrokerMqttIP,self.BrokerPort)
                    self.ClienteMQTT.loop_start()
                    self.ConectadoMQTT = True
                    self.PrintDebug ('Conectado al MQTT: '+ self.BrokerMqttIP)
                except:
                    self.ConectadoMQTT = False
                    self.PrintDebug ('Intento: ' + str (contador) + '. Error al conectar al servidor MQTT:' + str (self.BrokerMqttIP))
                    e = sys.exc_info()[1]
                        
            
    def LeerDatos(self):
      
        CadenaConexion = "http://" + self.InversorIP + "/measurements.xml"
        
        try:
            Documento = urlopen (CadenaConexion).read()
            DocumentoXML = etree.fromstring(Documento)
            
            for Mtag in DocumentoXML.iter('Measurement'):
                              
                opcion= Mtag.attrib["Type"]
                
                try:
                    valor =round (float (Mtag.attrib["Value"]),2)
                
                except:
                    valor = float (0)
                    self.PrintDebug ('valor cero del atributo '+ opcion)
                    
                if self.PublicarMqtt:
                    self.ClienteMQTT.publish (self.TopicMQTT +'/'+opcion,str(valor), qos=2, retain=False)
                
                if opcion == 'AC_Voltage':
                    self.AC_Voltage = valor        
                elif opcion=='AC_Current':
                    self.AC_Current= valor
                elif opcion=='AC_Power':
                    self.AC_Power = valor
                elif opcion=='AC_Power_fast':
                    self.AC_Power_fast = valor   
                elif opcion=='AC_Frequency':
                    self.AC_Frequency = valor
                elif opcion=='DC_Voltage1':
                    self.DC_Voltage1 = valor
                elif opcion=='DC_Voltage2':
                    self.DC_Voltage2 = valor
                elif opcion=='DC_Current1':
                    self.DC_Current1 = valor
                elif opcion=='DC_Current2':
                    self.DC_Current2 = valor
                elif opcion=='LINK_Voltage':
                    self.LINK_Voltage = valor
                elif opcion=='GridConsumedPower':
                    self.GridConsumedPower = valor
                elif opcion=='GridPower':
#                   Positivo significa que sale electricidad y negativo que entra a la red domestica  
                    self.GridPower = valor * -1
                elif opcion=='GridInjectedPower':
                    self.GridInjectedPower = valor
                elif opcion=='OwnConsumedPower':
                    self.OwnConsumedPower = valor
                elif opcion=='Derating':
                    self.Derating = valor
                else:
                    self.PrintDebug ('opcion no prevista: ' + opcion)      
            self.ConectadoInversor = True
        
        except:
            self.ConectadoInversor = False
            e = sys.exc_info()[1]
            self.PrintDebug(e.args[0])

                                
        if self.PublicarFreeDS:  
            Mensaje_Contador= '{"ENERGY":{"Power":' + str(self.GridPower)+ ',"Factor":0.0,"Frecuency":' + str (self.AC_Frequency) + ',"Voltage":'+ str (self.AC_Voltage) + ',"Current":' +str(self.AC_Current ) +'}}'
            self.ClienteMQTT.publish (self.TopicMeter, Mensaje_Contador, qos=2, retain=False)
            self.PrintDebug ('Mensaje_Contador: '+ Mensaje_Contador)
            #El inversor no devuelve la generación acumulada del dia Today=0.
            #El Power en DC de cada array se calcula multiplicando Intensidad por voltaje. Esto produce diferencias en relacion al Power en AC
            Mensaje_Solar = '{"ENERGY":{"Today": 0.0 ,"Power":' + str (self.AC_Power) + ',"Pv1Voltage":' + str (self.DC_Voltage1) + ',"Pv1Current":' + str (self.DC_Current1) + ',"Pv1Power":' + str(round(self.DC_Voltage1*self.DC_Current1,2))
            Mensaje_Solar = Mensaje_Solar + ',"Pv2Voltage":' + str(self.DC_Voltage2) +',"Pv2Current":' + str(self.DC_Current2) + ',"Pv2Power":' + str(round(self.DC_Voltage2*self.DC_Current2,2)) + ',"Temperature":0.0'  
            Mensaje_Solar = Mensaje_Solar + '}}'               
            self.ClienteMQTT.publish (self.TopicSolar, Mensaje_Solar, qos=2, retain=False)
            self.PrintDebug ('Mensaje_Solar' + Mensaje_Solar)
        
    
    def Arrancar (self):
        contador=1
        while True:
            self.PrintDebug ('Iteraciones:' + str (contador))
            self.LeerDatos()
            contador=contador+1
            time.sleep(self.FrecuenciaLectura)
            
        self.ClienteMQTT.disconnect()
        self.ClienteMQTT.loop_stop()
        
    def PrintDebug (self,texto):
        if self.Debug:
            try:
                ArchivoLog= open (self.path + "/PikoKostalInverter.log", "a")
                Hora=str (time.strftime("%H:%M:%S %d/%m/%y"))
                ArchivoLog.write (Hora + ': '+ texto + "\n")
                ArchivoLog.close ()
            except:
                print (texto)
                e = sys.exc_info()[1]
                error =str (e.args[0])
                print (error)
             
    
I = InversorKostal ()

I.Arrancar ()

         
            


