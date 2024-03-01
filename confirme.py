# -*- coding: utf-8 -*-

import os.path
from PyQt5 import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5 import QtCore, QtGui, QtWidgets    
from PyQt5.QtWidgets import (QCheckBox) 
from qgis.core import QgsSettings

class Ui_Dialog(object):
    def setupUi(self, Dialog, openParam):
        Dialog.openParam = openParam
        Dialog.setObjectName("Dialog")
        Dialog.resize(QtCore.QSize(QtCore.QRect(0,0,330,350).size()).expandedTo(Dialog.minimumSizeHint()))  
        width=500
        height=400
        Dialog.setFixedWidth(width)
        Dialog.setFixedHeight(height)
        
        self.gridlayout = QtWidgets.QGridLayout(Dialog)
        self.gridlayout.setObjectName("gridlayout")

        #self.label_2 = QtWidgets.QLabel(Dialog)

        
        self.labelImage = QtWidgets.QLabel(Dialog)
        myPath = os.path.dirname(__file__)+"/icons/extension.png";
        myDefPath = myPath.replace("\\","/");
        carIcon = QtGui.QImage(myDefPath)
        self.labelImage.setPixmap(QtGui.QPixmap.fromImage(carIcon))

        font = QtGui.QFont()
        font.setPointSize(15) 
        font.setWeight(50) 
        font.setBold(True)
                   
        label2 = QtWidgets.QLabel(self.tr("Consignes pour l'initialisation.\n\n\nCette application utilise une base de données POSTGRESQL/POSTGIS contenant les données DV3F et les tables utiles au programme (couche des communes, des départements et des EPCI, etc.), mais aussi les scripts et fonctions de calcul des indicateurs de marché.\n\nPour configurer la connexion au serveur, il faut renseigner les différents champs dans l'onglet 'Paramètres', en particulier les informations de connexion au serveur et le nom de la base de données."))
        label2.setWordWrap(True)
        self.gridlayout.addWidget(label2,0,2,1,1)  

        mObjetQCheckBox = QCheckBox()
        mObjetQCheckBox.setObjectName("mObjetQCheckBox")
        mObjetQCheckBox.setChecked(True if Dialog.openParam else False)
        self.gridlayout.addWidget(mObjetQCheckBox,2,1,1,1) 
        
        label = QtWidgets.QLabel(self.tr("Afficher cette fenêtre à l'ouverture de l'application.  "))
        self.gridlayout.addWidget(label,2,2,1,1)
        
        self.pushButton = QtWidgets.QPushButton(Dialog)
        self.pushButton.setObjectName("pushButton")
        self.gridlayout.addWidget(self.pushButton,2,3,1,1) 

        spacerItem = QtWidgets.QSpacerItem(20,20,QtWidgets.QSizePolicy.Minimum,QtWidgets.QSizePolicy.Expanding)
        self.gridlayout.addItem(spacerItem,1,1,1,1)

        self.retranslateUi(Dialog)
        self.pushButton.clicked.connect(Dialog.reject)
        mObjetQCheckBox.toggled.connect(lambda : self.functionObjetQCheckBox(mObjetQCheckBox, Dialog.openParam))       

    def functionObjetQCheckBox(self, mObjetQCheckBox, openParam):
        self.openParam = True if mObjetQCheckBox.isChecked() else False
        self.saveDialogParam()
        return 

    #Gestion dans le QGIS3.INI
    def saveDialogParam(self):
        mDicAutre = {}
        mSettings = QgsSettings()
        mSettings.beginGroup("QGIS_INOCC")
        #mSettings.beginGroup("Generale")
        mDicAutre["openParam"] = "true" if self.openParam else "false"
        #print(self.openParam)
        #print(mDicAutre["openParam"])

        for key, value in mDicAutre.items():
            mSettings.setValue(key, value)
        mSettings.endGroup()
        return 
    #Gestion dans le QGIS3.INI

    def retranslateUi(self, Dialog):
        Dialog.setWindowTitle(QtWidgets.QApplication.translate("Dialog", "Initialisation de l'application", None))
        self.pushButton.setText(QtWidgets.QApplication.translate("Dialog", "OK", None))

