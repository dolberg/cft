from flask import Flask
from flask_restful import Resource, Api, reqparse
import pandas as pd
import ast
import psycopg2
from datetime import datetime
from flask_cors import CORS

app = Flask(__name__)
api = Api(app)
cors = CORS(app, resources={r"/*": {"origns": "*"}})

class SuelosARG(Resource):
    

	def post(self):
		parser = reqparse.RequestParser()  # initialize

		parser.add_argument('centroid', required=True)  # add args
    
		args = parser.parse_args()  # parse arguments to dictionary
        
	# create new dataframe containing new values
		new_data = args['centroid']

		point= new_data
		conn = psycopg2.connect(
			port= 5432,
			host="localhost",
			database="suelosargentina",
			user="postgres",
			password="rfj751cc")


		cur= conn.cursor()
    
	# execute a statement
		cur.execute("SELECT text_sups1, drenaje_s1 FROM suelos_500000_v9 WHERE ST_intersects (geom, ST_GeomFromGeoJSON(%(point)s))",{"point":point})

		consulta=cur.fetchone()

		respuesta={
			'textura':consulta[0],
			'drenaje':consulta[1],
			'coordenadas': new_data
		}

	# display the PostgreSQL database consult
		return {'data': respuesta}, 200  # return data with 200 OK

  	# close the communication with the PostgreSQL
		cur.close()

class ConsultaAnexoCFT(Resource):
	def post(self):
		parser = reqparse.RequestParser()

		parser.add_argument('id', required=True)
		args = parser.parse_args() 

		reg=args['id']

		conn = psycopg2.connect(
			port= 5432,
			host="localhost",
			database="suelosargentina",
			user="postgres",
			password="rfj751cc")

		cur= conn.cursor()
		consultar=cur.execute("select * from anexo_cft where id= %(reg)s",{"reg": reg})
		consulta=cur.fetchone()

		respuesta={
			'id':consulta[0],
			'razonSocial': consulta[1],
    		'establecimiento': consulta[2],
    		'clima': consulta[3],
    		'materiaOrganica': consulta[4],
    		'humedad': consulta[5],
    		'pH': consulta[6],
    		'cambioUso': consulta[7],
    		'tiempoCambioUso': consulta[8],
    		'porcentajeCambioUso': consulta[9],
    		'cambioLabranza': consulta[10],
    		'tiempoCambioLabranza': consulta[11],
    		'porcentajeCambioLabranza': consulta[12],
    		'cambioCoberturas': consulta[13],
    		'tiempoCambioCoberturas': consulta[14],
    		'porcentajeCambioCoberturas': consulta[15],
    		'tratamientoRastrojos': consulta[16],
    		'clasificacionArboles': consulta[17],
    		'dapAnterior': consulta[18],
    		'densidadAnterior': consulta[19],
    		'dapActual': consulta[20],
    		'densidadActual': consulta[21],
		}

		return {'data': respuesta}, 200

		cur.close()

class FormulariosCFT(Resource):

	def post(self):

		parser = reqparse.RequestParser()  # initialize

		parser.add_argument('razonSocial', required=True)  # add args
		parser.add_argument('establecimiento', required=True)
		parser.add_argument('clima', required=True)
		parser.add_argument('materiaOrganica', required=True)
		parser.add_argument('humedad', required=True)
		parser.add_argument('pH', required=True)
		parser.add_argument('cambioUso', required=True)
		parser.add_argument('tiempoCambioUso', required=True)
		parser.add_argument('porcentajeCambioUso', required=True)
		parser.add_argument('cambioLabranza', required=True)
		parser.add_argument('tiempoCambioLabranza', required=True)
		parser.add_argument('porcentajeCambioLabranza', required=True)
		parser.add_argument('cambioCoberturas', required=True)
		parser.add_argument('tiempoCambioCoberturas', required=True)
		parser.add_argument('porcentajeCambioCoberturas', required=True)
		parser.add_argument('tratamientoRastrojos', required=True)
		parser.add_argument('clasificacionArboles', required=True)
		parser.add_argument('dapAnterior', required=True)
		parser.add_argument('densidadAnterior', required=True)
		parser.add_argument('dapActual', required=True)
		parser.add_argument('densidadActual', required=True)
    
		args = parser.parse_args()  # parse arguments to dictionary

		# create new dataframe containing new values
		
		razonSocial= args['razonSocial']
		establecimiento= args['establecimiento']
		clima= args['clima']
		materiaOrganica= args['materiaOrganica']
		humedad= args['humedad']
		pH= args['pH']
		cambioUso= args['cambioUso']
		tiempoCambioUso= args['tiempoCambioUso']
		porcentajeCambioUso= args['porcentajeCambioUso']
		cambioLabranza= args['cambioLabranza']
		tiempoCambioLabranza= args['tiempoCambioLabranza']
		porcentajeCambioLabranza= args['porcentajeCambioLabranza']
		cambioCoberturas= args['cambioCoberturas']
		tiempoCambioCoberturas= args['tiempoCambioCoberturas']
		porcentajeCambioCoberturas= args['porcentajeCambioCoberturas']
		tratamientoRastrojos= args['tratamientoRastrojos']
		clasificacionArboles= args['clasificacionArboles']
		dapAnterior= args['dapAnterior']
		densidadAnterior= args['densidadAnterior']
		dapActual= args['dapActual']
		densidadActual= args['densidadActual']

		conn = psycopg2.connect(
			port= 5432,
			host="localhost",
			database="suelosargentina",
			user="postgres",
			password="rfj751cc")


		cur= conn.cursor()
		contar=cur.execute("select * from anexo_cft where razonSocial= %(razonSocial)s and establecimiento = %(establecimiento)s",{"razonSocial": razonSocial,"establecimiento": establecimiento})
		contado=cur.fetchone()
		if contado is None:

			cur.execute("Insert into anexo_cft (razonSocial,establecimiento, clima, materiaOrganica, humedad, pH, cambioUso, tiempoCambioUso, porcentajeCambioUso, cambioLabranza, tiempoCambioLabranza, porcentajeCambioLabranza, cambioCoberturas, tiempoCambioCoberturas, porcentajeCambioCoberturas, tratamientoRastrojos, clasificacionArboles,dapAnterior,densidadAnterior, dapActual, densidadActual, created_at, updated_at) values (%(razonSocial)s, %(establecimiento)s, %(clima)s, %(materiaOrganica)s, %(humedad)s, %(pH)s, %(cambioUso)s, %(tiempoCambioUso)s, %(porcentajeCambioUso)s, %(cambioLabranza)s, %(tiempoCambioLabranza)s, %(porcentajeCambioLabranza)s, %(cambioCoberturas)s, %(tiempoCambioCoberturas)s, %(porcentajeCambioCoberturas)s, %(tratamientoRastrojos)s, %(clasificacionArboles)s, %(dapAnterior)s, %(densidadAnterior)s, %(dapActual)s, %(densidadActual)s, %(created_at)s, %(updated_at)s)", {
				"razonSocial": razonSocial,
    			"establecimiento": establecimiento,
    			"clima": clima,
    			"materiaOrganica": materiaOrganica,
    			"humedad": humedad,
    			"pH": pH,
    			"cambioUso": cambioUso,
    			"tiempoCambioUso": tiempoCambioUso,
    			"porcentajeCambioUso": porcentajeCambioUso,
    			"cambioLabranza": cambioLabranza,
    			"tiempoCambioLabranza": tiempoCambioLabranza,
    			"porcentajeCambioLabranza":porcentajeCambioLabranza,
    			"cambioCoberturas": cambioCoberturas,
    			"tiempoCambioCoberturas": tiempoCambioCoberturas,
    			"porcentajeCambioCoberturas": porcentajeCambioCoberturas,
    			"tratamientoRastrojos": tratamientoRastrojos,
    			"clasificacionArboles": clasificacionArboles,
    			"dapAnterior": dapAnterior,
    			"densidadAnterior": densidadAnterior,
    			"dapActual": dapActual,
    			"densidadActual": densidadActual,
    			"created_at": datetime.now(),
    			"updated_at": datetime.now()
			})

			conn.commit()
		else:

			cur.execute("update anexo_cft set clima=%(clima)s, materiaOrganica=%(materiaOrganica)s, humedad= %(humedad)s,pH= %(pH)s, cambioUso= %(cambioUso)s, tiempoCambioUso= %(tiempoCambioUso)s, porcentajeCambioUso= %(porcentajeCambioUso)s, cambioLabranza= %(cambioLabranza)s, tiempoCambioLabranza= %(tiempoCambioLabranza)s, porcentajeCambioLabranza= %(porcentajeCambioLabranza)s, cambioCoberturas= %(cambioCoberturas)s, tiempoCambioCoberturas= %(tiempoCambioCoberturas)s, porcentajeCambioCoberturas= %(porcentajeCambioCoberturas)s, tratamientoRastrojos= %(tratamientoRastrojos)s, clasificacionArboles= %(clasificacionArboles)s, dapAnterior= %(dapAnterior)s, densidadAnterior= %(densidadAnterior)s, dapActual= %(dapActual)s, densidadActual= %(densidadActual)s, updated_at = %(updated_at)s where razonSocial= %(razonSocial)s and establecimiento = %(establecimiento)s", {
				"razonSocial": razonSocial,
    			"establecimiento": establecimiento,
    			"clima":clima,
				"materiaOrganica": materiaOrganica,
    			"humedad": humedad,
    			"pH": pH,
    			"cambioUso": cambioUso,
    			"tiempoCambioUso": tiempoCambioUso,
    			"porcentajeCambioUso": porcentajeCambioUso,
    			"cambioLabranza": cambioLabranza,
    			"tiempoCambioLabranza": tiempoCambioLabranza,
    			"porcentajeCambioLabranza":porcentajeCambioLabranza,
    			"cambioCoberturas": cambioCoberturas,
    			"tiempoCambioCoberturas": tiempoCambioCoberturas,
    			"porcentajeCambioCoberturas": porcentajeCambioCoberturas,
    			"tratamientoRastrojos": tratamientoRastrojos,
    			"clasificacionArboles": clasificacionArboles,
    			"dapAnterior": dapAnterior,
    			"densidadAnterior": densidadAnterior,
    			"dapActual": dapActual,
    			"densidadActual": densidadActual,
    			"updated_at": datetime.now()
			})

			conn.commit()

		cur.execute("select * from anexo_cft where razonSocial= %(razonSocial)s and establecimiento = %(establecimiento)s order by updated_at desc fetch first row only",{
			"razonSocial": razonSocial,
    		"establecimiento": establecimiento
			})

		consulta=cur.fetchone()

		respuesta={
			'id':consulta[0],
			'razonSocial': consulta[1],
    		'establecimiento': consulta[2],
    		'clima': consulta[3],
    		'materiaOrganica': consulta[4],
    		'humedad': consulta[5],
    		'pH': consulta[6],
    		'cambioUso': consulta[7],
    		'tiempoCambioUso': consulta[8],
    		'porcentajeCambioUso': consulta[9],
    		'cambioLabranza': consulta[10],
    		'tiempoCambioLabranza': consulta[11],
    		'porcentajeCambioLabranza': consulta[12],
    		'cambioCoberturas': consulta[13],
    		'tiempoCambioCoberturas': consulta[14],
    		'porcentajeCambioCoberturas': consulta[15],
    		'tratamientoRastrojos': consulta[16],
    		'clasificacionArboles': consulta[17],
    		'dapAnterior': consulta[18],
    		'densidadAnterior': consulta[19],
    		'dapActual': consulta[20],
    		'densidadActual': consulta[21],
		}
	# display the PostgreSQL database consult
		return {'data': respuesta}, 200  # return data with 200 OK

  	# close the communication with the PostgreSQL
		cur.close()

class AgregarTexturaDrenaje(Resource):
    

	def post(self):
		parser = reqparse.RequestParser()  # initialize

		parser.add_argument('id', required=True)  # add args
		parser.add_argument('textura', required=True)
		parser.add_argument('drenaje', required=True) 
    
		args = parser.parse_args()  # parse arguments to dictionary

		reg=args['id']
		textura=args['textura']
		drenaje=args['drenaje']

		conn = psycopg2.connect(
			port= 5432,
			host="localhost",
			database="suelosargentina",
			user="postgres",
			password="rfj751cc")


		cur= conn.cursor()

		cur.execute("update anexo_cft set textura=%(textura)s, drenaje=%(drenaje)s, updated_at=%(updated_at)s where id=%(reg)s",{
			"reg":reg,
			"textura":textura,
			"drenaje":drenaje,
			"updated_at":datetime.now()
		})

		conn.commit()

api.add_resource(SuelosARG, '/suelos-arg')  
api.add_resource(FormulariosCFT, '/formularios-cft')
api.add_resource(ConsultaAnexoCFT, '/consulta-anexo-cft')
api.add_resource(AgregarTexturaDrenaje, '/agregar-carta')

if __name__ == '__main__':
	app.run()  # run our Flask app