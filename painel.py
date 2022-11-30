import streamlit as st
import sqlalchemy
import pymysql
import datetime
import math
import pandas as pd
import numpy as np


from sqlalchemy import and_, or_
from datetime import timedelta



class Singleton(type):

	# Referência: https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
	
	_instances = {}

	def __call__(cls, *args, **kwargs):

		if cls not in cls._instances:
			cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
		return cls._instances[cls]




class database(metaclass=Singleton):

	# Páginas importantes acessadas durante o desenvolvimento desta função.
	# 
	# Exemplos SQLAlchemy — Python Tutorial
	# https://towardsdatascience.com/sqlalchemy-python-tutorial-79a577141a91
	# 
	# Working with Engines and Connections [Acesso em 18 de julho de 2022]
	# https://docs.sqlalchemy.org/en/13/core/connections.html
	#
	# Using Connection Pools with Multiprocessing or os.fork() [Acesso em 18 de julho de 2022]
	# https://docs.sqlalchemy.org/en/13/core/pooling.html#pooling-multiprocessing


	__engine = None

	# Initializing 
	def __init__( self ):

		dialectdriver = 'mysql+pymysql'

		user     = st.secrets["user"]
		passwd   = st.secrets["passwd"]
		host     = st.secrets["host"]
		port     = st.secrets["port"]
		database = st.secrets["db"]

		connectionString = '{dialectdriver}://{user}:{passwd}@{host}:{port}/{db}'.format( dialectdriver=dialectdriver, user=user, passwd=passwd, host=host, port=port, db=database )

		self.__engine = sqlalchemy.create_engine( connectionString )


	def getEngine(self):
		return self.__engine





def consultaPlanos100kbps( INEP ):

	myDatabase = database()
	engine = myDatabase.getEngine()

	with engine.connect() as connection:

		query = "SELECT CoordenadoresArticuladores10112022.CodigoINEP, viewCustoTotalInstalacao.CEP, CoordenadoresArticuladores10112022.NomeEscola, viewCustoTotalInstalacao.dataInfoPrimaryInMega AS Velocidade, viewCustoTotalInstalacao.primeiraParteNomePlano, MIN(viewCustoTotalInstalacao.dataPriceUnit) AS MenorValor,  viewCustoTotalInstalacao.custoTotalInstalacao, CoordenadoresArticuladores10112022.Matriculas, viewCustoTotalInstalacao.dataInfoPrimaryInMega, viewCustoTotalInstalacao.custoTotalPrimeiroAno, CoordenadoresArticuladores10112022.Custeio \
				 FROM       `mega-edu-scraping-melhorplano`.CoordenadoresArticuladores10112022 \
				 INNER JOIN `mega-edu-scraping-melhorplano`.CEPs \
				 INNER JOIN `mega-edu-scraping-melhorplano`.viewCustoTotalInstalacao \
				 	ON  CoordenadoresArticuladores10112022.CodigoINEP = CEPs.co_entidade \
				 	AND CEPs.co_cep = viewCustoTotalInstalacao.CEP \
				 WHERE CoordenadoresArticuladores10112022.Matriculas * 0.1 <= viewCustoTotalInstalacao.dataInfoPrimaryInMega \
				 	AND viewCustoTotalInstalacao.custoTotalPrimeiroAno <= CoordenadoresArticuladores10112022.Custeio \
				 	AND CoordenadoresArticuladores10112022.CodigoINEP = {CodigoINEP} \
				 GROUP BY viewCustoTotalInstalacao.CEP, viewCustoTotalInstalacao.primeiraParteNomePlano \
				 ORDER BY Velocidade DESC, MenorValor".format( CodigoINEP = INEP)
	
		return pd.read_sql(query, con=engine)





if __name__ == "__main__":


	dfResult = None

	with st.sidebar:

		st.title("Filtros")

		consultas = ('Planos sugeridos', 'Todos os planos')

		consulta = st.selectbox('Qual consulta deseja realizar?', consultas)

		if consulta == consultas[0]:

			add_radio = st.radio("Escolha o valor de referência para velocidade.", ("1 Mbps", "100 kbps") )
			INEP = st.text_input('Informe o INEP')

			if st.button('Buscar'):

				if INEP != "":
					dfResult = consultaPlanos100kbps( INEP )
				else:
					st.error('Informe o INEP')


	st.dataframe(dfResult)