import azure.functions as func
import logging
import json
import requests
from datetime import datetime

app = func.FunctionApp()

# Databricks configuration
DATABRICKS_CONFIG = {
    'host': 'dbc-a3122420-cda0.cloud.databricks.com',
    'token': 'dapiebca5706c83ec913f50c0ab9e15e1e8c',
    'warehouse_id': 'b0b9bd45f760040f',
    'catalog': 'gebouwdossier',
    'schema': 'testsql',
    'table': 'test'
}

@app.route(route="order", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def create_order(req: func.HttpRequest) -> func.HttpResponse:
    """Receive order data and send it to Databricks"""
    logging.info('📦 Order endpoint called')
    
    try:
        order_data = req.get_json()
        
        logging.info(f"🚀 NIEUWE BESTELLING ONTVANGEN!")
        logging.info(f"Order ID: {order_data.get('orderId', 'N/A')}")
        logging.info(f"Model: {order_data.get('modelnaam', 'N/A')}")
        logging.info(f"Locatie: {order_data.get('locatie', 'N/A')}")
        logging.info(f"Prijs: €{order_data.get('prijs', 'N/A')}")
        
        # Create SQL statement for gebouwdossier.testsql.test
        # Based on: INSERT INTO gebouwdossier.testsql.test VALUES (id, 'name', value)
        sql_statement = f"""
            INSERT INTO {DATABRICKS_CONFIG['catalog']}.{DATABRICKS_CONFIG['schema']}.{DATABRICKS_CONFIG['table']} VALUES
            ({hash(order_data['orderId']) % 1000000}, '{escape_sql(order_data['modelnaam'])}', {order_data['prijs']})
        """
        
        # Databricks SQL API endpoint
        api_url = f"https://{DATABRICKS_CONFIG['host']}/api/2.0/sql/statements"
        
        request_body = {
            'warehouse_id': DATABRICKS_CONFIG['warehouse_id'],
            'statement': sql_statement,
            'wait_timeout': '30s'
        }
        
        logging.info(f"Sending to Databricks: {sql_statement}")
        
        # Make request to Databricks
        response = requests.post(
            api_url,
            headers={
                'Authorization': f"Bearer {DATABRICKS_CONFIG['token']}",
                'Content-Type': 'application/json'
            },
            json=request_body,
            timeout=35
        )
        
        if response.ok:
            result = response.json()
            logging.info(f"✅ Success! Databricks response: {result.get('status', {}).get('state')}")
            
            return func.HttpResponse(
                json.dumps({
                    'success': True,
                    'message': 'Order successfully saved to Databricks',
                    'orderId': order_data['orderId'],
                    'databricks_response': result
                }),
                status_code=200,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        else:
            error_text = response.text
            logging.error(f"❌ Databricks error: {response.status_code} - {error_text}")
            
            return func.HttpResponse(
                json.dumps({
                    'success': False,
                    'error': f"Databricks error: {response.status_code}",
                    'details': error_text
                }),
                status_code=response.status_code,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
            
    except Exception as e:
        logging.error(f"❌ Exception: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                'success': False,
                'error': str(e)
            }),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )


@app.route(route="swap", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def swap_installation(req: func.HttpRequest) -> func.HttpResponse:
    """Receive swap request from werkorder app"""
    logging.info('🔄 Swap endpoint called')
    
    try:
        swap_data = req.get_json()
        
        logging.info("🔄 REGELKAST WISSEL ACTIE!")
        logging.info(f"Van: {swap_data.get('currentModel', 'N/A')}")
        logging.info(f"Naar: {swap_data.get('newModel', 'N/A')}")
        logging.info(f"Locatie: {swap_data.get('location', 'N/A')}")
        logging.info(f"Prijs nieuwe kast: €{swap_data.get('price', 'N/A')}")
        
        # TODO: Later hier Databricks update doen
        
        return func.HttpResponse(
            json.dumps({
                'success': True,
                'message': 'Regelkast succesvol gewisseld',
                'timestamp': swap_data.get('timestamp')
            }),
            status_code=200,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
        
    except Exception as e:
        logging.error(f"❌ Error bij swap: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                'success': False,
                'error': str(e)
            }),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )


@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint"""
    return func.HttpResponse(
        json.dumps({'status': 'healthy', 'service': 'heijmans-demo-api'}),
        status_code=200,
        mimetype="application/json",
        headers={"Access-Control-Allow-Origin": "*"}
    )


def escape_sql(value):
    """Escape single quotes for SQL"""
    return value.replace("'", "''")
