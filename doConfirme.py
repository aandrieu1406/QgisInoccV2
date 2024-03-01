# -*- coding: iso-8859-1 -*-

from PyQt5.QtCore import *
from PyQt5.QtGui import *
from qgis.core import *
from PyQt5.QtWidgets import *
#import de la classe boîte de dialogue "A propos ..."
from .confirme import Ui_Dialog

    
class Dialog(QDialog, Ui_Dialog):
	def __init__(self, openParam):
		QDialog.__init__(self)
		self.setupUi(self, openParam)
		
        #Quand fermeture de l boite de dialogue de paramètrage
        #self.returnAndSaveDialogParam("Save")
