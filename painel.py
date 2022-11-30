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




def consultaTodosPlanos( INEP ):

	myDatabase = database()
	engine = myDatabase.getEngine()

	with engine.connect() as connection:

		query = "SELECT CEPs.co_entidade, Planos.* \
				 FROM `mega-edu-scraping-melhorplano`.Planos \
				 INNER JOIN `mega-edu-scraping-melhorplano`.CEPs \
				 	ON Planos.CEP = CEPs.co_cep \
				 	AND CEPs.co_entidade = {CodigoINEP}".format( CodigoINEP = INEP)
	
		return pd.read_sql(query, con=engine)





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





def consultaPlanos1Mbps( INEP ):

	myDatabase = database()
	engine = myDatabase.getEngine()

	with engine.connect() as connection:

		query = "SELECT PlanosMaisBaratosPorCep.INEP, planos.CEP, PlanosMaisBaratosPorCep.custoTotalInstalacao, PlanosMaisBaratosPorCep.dataInfoPrimaryInMega AS Velocidade, planos.dataPriceUnit AS CustoMensal, planos.nomeCompletoPlano, planos.primeiraParteNomePlano, PlanosMaisBaratosPorCep.Escola \
			     FROM `mega-edu-scraping-melhorplano`.Planos AS planos \
			     JOIN \
			     ( \
			     	SELECT EscolasEMaiorTurno01112022.INEP, viewCustoTotalInstalacao.CEP, EscolasEMaiorTurno01112022.Escola , viewCustoTotalInstalacao.custoTotalInstalacao,  viewCustoTotalInstalacao.dataInfoPrimaryInMega , viewCustoTotalInstalacao.primeiraParteNomePlano, MIN(viewCustoTotalInstalacao.dataPriceUnit) AS MenorValor \
			     	FROM       `mega-edu-scraping-melhorplano`.EscolasEMaiorTurno01112022 \
			     	INNER JOIN `mega-edu-scraping-melhorplano`.CoordenadoresArticuladores10112022 \
			     	INNER JOIN `mega-edu-scraping-melhorplano`.CEPs \
			     	INNER JOIN `mega-edu-scraping-melhorplano`.viewCustoTotalInstalacao \
			     		ON  viewCustoTotalInstalacao.CEP = CEPs.co_cep \
			     		AND CEPs.co_entidade = EscolasEMaiorTurno01112022.INEP \
			     		AND CoordenadoresArticuladores10112022.CodigoINEP = EscolasEMaiorTurno01112022.INEP \
			     	WHERE EscolasEMaiorTurno01112022.MatriculasNoMaiorTurno * 0.1 <= viewCustoTotalInstalacao.dataInfoPrimaryInMega \
			     		AND viewCustoTotalInstalacao.custoTotalPrimeiroAno <= CoordenadoresArticuladores10112022.Custeio \
			     		AND EscolasEMaiorTurno01112022.INEP = {CodigoINEP} \
			     	GROUP BY viewCustoTotalInstalacao.CEP, viewCustoTotalInstalacao.primeiraParteNomePlano \
			     ) PlanosMaisBaratosPorCep \
			     	ON PlanosMaisBaratosPorCep.CEP = planos.CEP \
			     	AND PlanosMaisBaratosPorCep.primeiraParteNomePlano = planos.primeiraParteNomePlano \
			     	AND PlanosMaisBaratosPorCep.MenorValor = planos.dataPriceUnit \
			     ORDER BY Velocidade DESC, MenorValor".format( CodigoINEP = INEP)
	
		return pd.read_sql(query, con=engine)






def check_password():
    """Returns `True` if the user had the correct password."""

	# Páginas importantes acessadas durante o desenvolvimento desta função.
	# 
	# Authentication without SSO
	# https://docs.streamlit.io/knowledge-base/deploy/authentication-without-sso

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True




if __name__ == "__main__":


	if check_password():

		dfResult = None

		with st.sidebar:

			st.title("Filtros")

			consultas = ('Planos sugeridos', 'Todos os planos')

			consulta = st.selectbox('Qual consulta deseja realizar?', consultas)

			if consulta == consultas[0]:

				velocidades = ("1 Mbps", "100 kbps")

				velocidade = st.radio("Escolha o valor de referência para velocidade.", velocidades )
				INEP = st.text_input('Informe o INEP')

				if st.button('Buscar'):

					if INEP != "":
						if velocidade == velocidades[0]:
							dfResult = consultaPlanos1Mbps( INEP )
						elif velocidade == velocidades[1]:
							dfResult = consultaPlanos100kbps( INEP )
					else:
						st.error('🚨 Informe o INEP')

			elif consulta == consultas[1]:
				INEP = st.text_input('Informe o INEP')

				if st.button('Buscar'):

					if INEP != "":
						dfResult = consultaTodosPlanos( INEP )


		if dfResult is not None:
			st.title("Resultados")
			st.dataframe(dfResult)